use anyhow::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{self, Duration};
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use crate::memory::Shared;


#[derive(Debug, Clone)]
pub struct Update {
    pub shape: Vec<u32>,
    pub start: u32,
    pub len: u32,
}

#[derive(Debug, Clone)]
pub struct UpdatePacket {
    pub updates: Vec<Update>,
    pub meta: Option<String>,
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

pub fn packet_to_bytes(packet: &UpdatePacket, state: &Shared) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(packet.updates.len() as u32).to_le_bytes());
    for u in &packet.updates {
        buf.extend_from_slice(&u.to_bytes(state));
    }
    if let Some(meta) = &packet.meta {
        let bytes = meta.as_bytes();
        buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(bytes);
    } else {
        buf.extend_from_slice(&(0u32.to_le_bytes()));
    }
    buf
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

async fn read_update_set(sock: &mut TcpStream) -> Result<Option<(Vec<(Vec<usize>, usize, Vec<f64>)>, Option<String>)>> {
    let mut len_buf = [0u8; 4];
    if !read_exact_checked(sock, &mut len_buf).await? {
        return Ok(None);
    }
    let count = u32::from_le_bytes(len_buf) as usize;
    let mut updates = Vec::with_capacity(count);
    for _ in 0..count {
        let (shape, start_idx, val_len) = match read_update_header(sock).await? {
            Some(v) => v,
            None => return Ok(None),
        };
        let vals = match read_values(sock, val_len).await? {
            Some(v) => v,
            None => return Ok(None),
        };
        updates.push((shape, start_idx, vals));
    }
    if !read_exact_checked(sock, &mut len_buf).await? {
        return Ok(None);
    }
    let meta_len = u32::from_le_bytes(len_buf) as usize;
    let meta = if meta_len > 0 {
        let mut buf = vec![0u8; meta_len];
        if !read_exact_checked(sock, &mut buf).await? {
            return Ok(None);
        }
        Some(String::from_utf8_lossy(&buf).to_string())
    } else {
        None
    };

    Ok(Some((updates, meta)))
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

pub async fn handle_peer(mut sock: TcpStream, state: Shared, meta: Arc<Mutex<Vec<String>>>) -> Result<()> {
    let addr = sock.peer_addr().ok();
    println!("peer {:?} connected", addr);
    let local_shape = state.shape().to_vec();
    let len: usize = local_shape.iter().product();

    loop {
        let packet = match read_update_set(&mut sock).await? {
            Some(v) => v,
            None => break,
        };
        let (updates, metadata) = packet;
        for (shape, start_idx, vals) in updates {
            if shape != local_shape {
                println!("shape mismatch: recv {:?} local {:?}", shape, state.shape());
                continue;
            }
            apply_update(&state, start_idx, &vals, len)?;
        }
        if let Some(m) = metadata {
            let mut q = meta.lock().unwrap();
            q.push(m);
        }
    }
    println!("peer {:?} disconnected", addr);
    Ok(())
}

pub async fn serve(
    addr: SocketAddr,
    rx: async_channel::Receiver<UpdatePacket>,
    state: Shared,
    pending_meta: Arc<Mutex<Option<String>>>,
) -> Result<()> {
    println!("listening on {}", addr);
    let lst = TcpListener::bind(addr).await?;
    let mut conns: Vec<TcpStream> = Vec::new();
    loop {
        tokio::select! {
            res = lst.accept() => {
                let (mut sock, peer) = res?;
                println!("accepted connection from {}", peer);
                let snap = snapshot_update(&state);
                let meta = pending_meta.lock().unwrap().clone();
                let data = packet_to_bytes(&UpdatePacket{updates: vec![snap], meta}, &state);
                if sock.write_all(&data).await.is_ok() {
                    conns.push(sock);
                }
            }
            res = rx.recv() => {
                let u = match res { Ok(v) => v, Err(_) => break };
                let data = packet_to_bytes(&u, &state);
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

pub async fn client(server: SocketAddr, state: Shared, meta: Arc<Mutex<Vec<String>>>) -> Result<()> {
    let mut interval = time::interval(Duration::from_secs(1));
    loop {
        println!("connecting to server {}", server);
        match TcpStream::connect(server).await {
            Ok(sock) => {
                println!("connected to {}", server);
                let res = handle_peer(sock, state.clone(), meta.clone()).await;
                if let Err(e) = res {
                    println!("connection error {}: {}", server, e);
                }
            }
            Err(e) => println!("failed to connect to {}: {}", server, e),
        }
        interval.tick().await;
    }
}

