use anyhow::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use std::io::ErrorKind;
use std::net::SocketAddr;

use crate::memory::Shared;


#[derive(Debug, Clone)]
pub struct Update {
    pub shape: Vec<u32>,
    pub start: u32,
    pub vals: Vec<f64>,
}

impl Update {
    pub fn to_bytes(&self) -> Vec<u8> {
        let shape_len = self.shape.len() as u32;
        let val_len = self.vals.len() as u32;
        let mut buf = Vec::with_capacity(4 + self.shape.len() * 4 + 4 + 4 + self.vals.len() * 8);
        buf.extend_from_slice(&shape_len.to_le_bytes());
        for d in &self.shape {
            buf.extend_from_slice(&d.to_le_bytes());
        }
        buf.extend_from_slice(&self.start.to_le_bytes());
        buf.extend_from_slice(&val_len.to_le_bytes());
        for v in &self.vals {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        buf
    }
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

pub async fn broadcaster(peers: Vec<SocketAddr>, rx: async_channel::Receiver<Update>) -> Result<()> {
    let mut conns: Vec<(SocketAddr, TcpStream)> = Vec::new();
    for p in &peers {
        println!("connecting to peer {}", p);
        match TcpStream::connect(*p).await {
            Ok(s) => {
                println!("connected to {}", p);
                conns.push((*p, s));
            }
            Err(e) => println!("failed to connect to {}: {}", p, e),
        }
    }
    while let Some(u) = rx.recv().await.ok() {
        println!(
            "broadcasting update shape {:?} start {} len {}",
            u.shape,
            u.start,
            u.vals.len()
        );
        let data = u.to_bytes();

        for p in &peers {
            if !conns.iter().any(|(addr, _)| addr == p) {
                println!("reconnecting to {}", p);
                match TcpStream::connect(*p).await {
                    Ok(s) => {
                        println!("connected to {}", p);
                        conns.push((*p, s));
                    }
                    Err(e) => println!("connect failed to {}: {}", p, e),
                }
            }
        }

        let mut alive = Vec::new();
        for (addr, mut s) in conns {
            match s.write_all(&data).await {
                Ok(_) => {
                    println!("sent to {}", addr);
                    alive.push((addr, s));
                }
                Err(e) => {
                    println!("send failed to {}: {}", addr, e);
                }
            }
        }
        conns = alive;
    }
    Ok(())
}

pub async fn listener(addr: SocketAddr, state: Shared) -> Result<()> {
    println!("listening on {}", addr);
    let lst = TcpListener::bind(addr).await?;
    loop {
        let (sock, peer) = lst.accept().await?;
        println!("accepted connection from {}", peer);
        let st = state.clone();
        tokio::spawn(async move { let _ = handle_peer(sock, st).await; });
    }
}

