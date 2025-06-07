use anyhow::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{self, Duration};
use std::io::ErrorKind;
use std::net::SocketAddr;

use crate::memory::Shared;


#[derive(Debug, Clone)]
pub struct Update {
    pub shape: Vec<u32>,
    pub start: u32,
    pub len: u32,
}

impl Update {
    pub fn to_bytes(&self, state: &Shared) -> Vec<u8> {
        let shape_len = self.shape.len() as u32;
        let mut buf = Vec::with_capacity(4 + self.shape.len() * 4 + 4 + 4 + self.len as usize * 8);
        buf.extend_from_slice(&shape_len.to_le_bytes());
        for d in &self.shape {
            buf.extend_from_slice(&d.to_le_bytes());
        }
        buf.extend_from_slice(&self.start.to_le_bytes());
        buf.extend_from_slice(&self.len.to_le_bytes());
        let start = self.start as usize;
        for i in 0..self.len as usize {
            let v = state.get(start + i);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        buf
    }
}

fn snapshot_update(state: &Shared) -> Update {
    let shape: Vec<u32> = state.shape().iter().map(|&d| d as u32).collect();
    let len: usize = state.shape().iter().product();
    Update { shape, start: 0, len: len as u32 }
}

async fn read_exact_checked(sock: &mut TcpStream, buf: &mut [u8]) -> Result<bool> {
    match sock.read_exact(buf).await {
        Ok(_) => Ok(true),
        Err(ref e) if e.kind() == ErrorKind::UnexpectedEof => Ok(false),
        Err(ref e) if e.kind() == ErrorKind::ConnectionReset => Ok(false),
        Err(e) => Err(e.into()),
    }
}

async fn read_update_header(sock: &mut TcpStream) -> Result<Option<(Vec<usize>, usize, usize)>> {
    let mut len_buf = [0u8; 4];
    if !read_exact_checked(sock, &mut len_buf).await? {
        return Ok(None);
    }
    let shape_len = u32::from_le_bytes(len_buf) as usize;

    let mut shape_bytes = vec![0u8; shape_len * 4];
    if !read_exact_checked(sock, &mut shape_bytes).await? {
        return Ok(None);
    }
    let mut shape = Vec::with_capacity(shape_len);
    for i in 0..shape_len {
        let mut d = [0u8; 4];
        d.copy_from_slice(&shape_bytes[i * 4..(i + 1) * 4]);
        shape.push(u32::from_le_bytes(d) as usize);
    }

    if !read_exact_checked(sock, &mut len_buf).await? {
        return Ok(None);
    }
    let start_idx = u32::from_le_bytes(len_buf) as usize;

    if !read_exact_checked(sock, &mut len_buf).await? {
        return Ok(None);
    }
    let val_len = u32::from_le_bytes(len_buf) as usize;

    Ok(Some((shape, start_idx, val_len)))
}

async fn read_values(sock: &mut TcpStream, len: usize) -> Result<Option<Vec<f64>>> {
    let mut buf = vec![0u8; len * 8];
    if !read_exact_checked(sock, &mut buf).await? {
        return Ok(None);
    }
    let mut vals = Vec::with_capacity(len);
    for i in 0..len {
        let mut b = [0u8; 8];
        b.copy_from_slice(&buf[i * 8..(i + 1) * 8]);
        vals.push(f64::from_le_bytes(b));
    }
    Ok(Some(vals))
}

async fn discard_values(sock: &mut TcpStream, len: usize) -> Result<bool> {
    let mut buf = vec![0u8; len * 8];
    read_exact_checked(sock, &mut buf).await
}

fn apply_update(state: &Shared, start_idx: usize, vals: &[f64], len: usize) -> Result<()> {
    state.start_write()?;
    let base = state.mm.mm.as_ptr() as *mut f64;
    for (i, v) in (start_idx..start_idx + vals.len()).zip(vals.iter()) {
        if i < len {
            unsafe { *base.add(i) = *v; }
        }
    }
    state.end_write()
}

pub async fn handle_peer(mut sock: TcpStream, state: Shared) -> Result<()> {
    let addr = sock.peer_addr().ok();
    println!("peer {:?} connected", addr);
    let local_shape = state.shape().to_vec();
    let len: usize = local_shape.iter().product();

    loop {
        let (shape, start_idx, val_len) = match read_update_header(&mut sock).await? {
            Some(v) => v,
            None => break,
        };

        if shape != local_shape {
            println!("shape mismatch: recv {:?} local {:?}", shape, state.shape());
            if !discard_values(&mut sock, val_len).await? { break; }
            continue;
        }

        let vals = match read_values(&mut sock, val_len).await? {
            Some(v) => v,
            None => break,
        };

        apply_update(&state, start_idx, &vals, len)?;
    }
    println!("peer {:?} disconnected", addr);
    Ok(())
}

pub async fn serve(addr: SocketAddr, rx: async_channel::Receiver<Update>, state: Shared) -> Result<()> {
    println!("listening on {}", addr);
    let lst = TcpListener::bind(addr).await?;
    let mut conns: Vec<TcpStream> = Vec::new();
    loop {
        tokio::select! {
            res = lst.accept() => {
                let (mut sock, peer) = res?;
                println!("accepted connection from {}", peer);
                let snap = snapshot_update(&state);
                if sock.write_all(&snap.to_bytes(&state)).await.is_ok() {
                    conns.push(sock);
                }
            }
            res = rx.recv() => {
                let u = match res { Ok(v) => v, Err(_) => break };
                let data = u.to_bytes(&state);
                let mut alive = Vec::new();
                for mut s in conns {
                    match s.write_all(&data).await {
                        Ok(_) => alive.push(s),
                        Err(e) => println!("send failed: {}", e),
                    }
                }
                conns = alive;
            }
        }
    }
    Ok(())
}

pub async fn client(server: SocketAddr, state: Shared) -> Result<()> {
    let mut interval = time::interval(Duration::from_secs(1));
    loop {
        println!("connecting to server {}", server);
        match TcpStream::connect(server).await {
            Ok(sock) => {
                println!("connected to {}", server);
                let res = handle_peer(sock, state.clone()).await;
                if let Err(e) = res {
                    println!("connection error {}: {}", server, e);
                }
            }
            Err(e) => println!("failed to connect to {}: {}", server, e),
        }
        interval.tick().await;
    }
}

