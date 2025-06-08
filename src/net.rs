use anyhow::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{self, Duration};
use std::collections::HashMap;
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

use crate::memory::Shared;


#[derive(Debug, Clone)]
pub struct Update {
    pub start: u32,
    pub len: u32,
}

#[derive(Debug, Clone)]
pub struct UpdatePacket {
    pub updates: Vec<Update>,
    pub meta: Option<String>,
}

#[derive(Debug, Clone)]
pub enum Target {
    Region(Vec<u32>),
    Named(String),
}

#[derive(Debug, Clone)]
pub struct Mapping {
    pub server_start: Vec<u32>,
    pub shape: Vec<u32>,
    pub target: Target,
}

#[derive(Debug, Clone)]
pub struct Subscription {
    pub client_shape: Vec<u32>,
    pub maps: Vec<Mapping>,
}



fn filtered_to_bytes(updates: &[FilteredUpdate], meta: &Option<String>) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(updates.len() as u32).to_le_bytes());
    for u in updates {
        buf.extend_from_slice(&(u.shape.len() as u32).to_le_bytes());
        for d in &u.shape {
            buf.extend_from_slice(&d.to_le_bytes());
        }
        buf.extend_from_slice(&u.start.to_le_bytes());
        buf.extend_from_slice(&(u.values.len() as u32).to_le_bytes());
        for v in &u.values {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        if let Some(name) = &u.name {
            buf.extend_from_slice(&(name.len() as u32).to_le_bytes());
            buf.extend_from_slice(name.as_bytes());
        } else {
            buf.extend_from_slice(&(0u32.to_le_bytes()));
        }
    }
    if let Some(meta) = meta {
        let bytes = meta.as_bytes();
        buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(bytes);
    } else {
        buf.extend_from_slice(&(0u32.to_le_bytes()));
    }
    buf
}

pub fn subscription_to_bytes(sub: &Subscription) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(1u8); // message type 1 for subscription
    buf.extend_from_slice(&(sub.client_shape.len() as u32).to_le_bytes());
    for d in &sub.client_shape {
        buf.extend_from_slice(&d.to_le_bytes());
    }
    buf.extend_from_slice(&(sub.maps.len() as u32).to_le_bytes());
    for m in &sub.maps {
        buf.extend_from_slice(&(m.server_start.len() as u32).to_le_bytes());
        for v in &m.server_start {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        buf.extend_from_slice(&(m.shape.len() as u32).to_le_bytes());
        for v in &m.shape {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        match &m.target {
            Target::Region(cs) => {
                buf.push(0u8);
                buf.extend_from_slice(&(cs.len() as u32).to_le_bytes());
                for v in cs {
                    buf.extend_from_slice(&v.to_le_bytes());
                }
            }
            Target::Named(name) => {
                buf.push(1u8);
                buf.extend_from_slice(&(name.len() as u32).to_le_bytes());
                buf.extend_from_slice(name.as_bytes());
            }
        }
    }
    buf
}

fn snapshot_update(state: &Shared) -> Update {
    let len: usize = state.shape().iter().product();
    Update { start: 0, len: len as u32 }
}

async fn read_exact_checked(sock: &mut TcpStream, buf: &mut [u8]) -> Result<bool> {
    match sock.read_exact(buf).await {
        Ok(_) => Ok(true),
        Err(ref e) if e.kind() == ErrorKind::UnexpectedEof => Ok(false),
        Err(ref e) if e.kind() == ErrorKind::ConnectionReset => Ok(false),
        Err(e) => Err(e.into()),
    }
}

async fn read_subscription(sock: &mut TcpStream) -> Result<Option<Subscription>> {
    let mut typ = [0u8; 1];
    if !read_exact_checked(sock, &mut typ).await? {
        return Ok(None);
    }
    if typ[0] != 1u8 {
        return Ok(None);
    }

    let mut len_buf = [0u8; 4];
    if !read_exact_checked(sock, &mut len_buf).await? { return Ok(None); }
    let shape_len = u32::from_le_bytes(len_buf) as usize;
    let mut shape_bytes = vec![0u8; shape_len * 4];
    if !read_exact_checked(sock, &mut shape_bytes).await? { return Ok(None); }
    let mut shape = Vec::with_capacity(shape_len);
    for i in 0..shape_len {
        let mut d = [0u8;4];
        d.copy_from_slice(&shape_bytes[i*4..(i+1)*4]);
        shape.push(u32::from_le_bytes(d));
    }
    if !read_exact_checked(sock, &mut len_buf).await? { return Ok(None); }
    let map_len = u32::from_le_bytes(len_buf) as usize;
    let mut maps = Vec::with_capacity(map_len);
    for _ in 0..map_len {
        if !read_exact_checked(sock, &mut len_buf).await? { return Ok(None); }
        let ss_len = u32::from_le_bytes(len_buf) as usize;
        let mut buf = vec![0u8; ss_len * 4];
        if !read_exact_checked(sock, &mut buf).await? { return Ok(None); }
        let mut server_start = Vec::with_capacity(ss_len);
        for i in 0..ss_len {
            let mut b = [0u8;4];
            b.copy_from_slice(&buf[i*4..(i+1)*4]);
            server_start.push(u32::from_le_bytes(b));
        }

        if !read_exact_checked(sock, &mut len_buf).await? { return Ok(None); }
        let sh_len = u32::from_le_bytes(len_buf) as usize;
        let mut buf = vec![0u8; sh_len * 4];
        if !read_exact_checked(sock, &mut buf).await? { return Ok(None); }
        let mut region_shape = Vec::with_capacity(sh_len);
        for i in 0..sh_len {
            let mut b = [0u8;4];
            b.copy_from_slice(&buf[i*4..(i+1)*4]);
            region_shape.push(u32::from_le_bytes(b));
        }

        let mut kind = [0u8;1];
        if !read_exact_checked(sock, &mut kind).await? { return Ok(None); }
        let target = if kind[0] == 0u8 {
            if !read_exact_checked(sock, &mut len_buf).await? { return Ok(None); }
            let cs_len = u32::from_le_bytes(len_buf) as usize;
            let mut buf = vec![0u8; cs_len * 4];
            if !read_exact_checked(sock, &mut buf).await? { return Ok(None); }
            let mut cs = Vec::with_capacity(cs_len);
            for i in 0..cs_len {
                let mut b = [0u8;4];
                b.copy_from_slice(&buf[i*4..(i+1)*4]);
                cs.push(u32::from_le_bytes(b));
            }
            Target::Region(cs)
        } else {
            if !read_exact_checked(sock, &mut len_buf).await? { return Ok(None); }
            let nlen = u32::from_le_bytes(len_buf) as usize;
            let mut buf = vec![0u8; nlen];
            if !read_exact_checked(sock, &mut buf).await? { return Ok(None); }
            let name = String::from_utf8_lossy(&buf).to_string();
            Target::Named(name)
        };
        maps.push(Mapping { server_start, shape: region_shape, target });
    }
    Ok(Some(Subscription { client_shape: shape, maps }))
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

async fn read_update_set(sock: &mut TcpStream) -> Result<Option<(Vec<(Vec<usize>, usize, Vec<f64>, Option<String>)>, Option<String>)>> {
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
        if !read_exact_checked(sock, &mut len_buf).await? { return Ok(None); }
        let name_len = u32::from_le_bytes(len_buf) as usize;
        let name = if name_len > 0 {
            let mut buf = vec![0u8; name_len];
            if !read_exact_checked(sock, &mut buf).await? { return Ok(None); }
            Some(String::from_utf8_lossy(&buf).to_string())
        } else { None };
        updates.push((shape, start_idx, vals, name));
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

fn strides(shape: &[u32]) -> Vec<usize> {
    let mut s = vec![1usize; shape.len()];
    for i in (0..shape.len()-1).rev() {
        s[i] = s[i+1] * shape[i+1] as usize;
    }
    s
}

fn gather_segments(
    dim: usize,
    prefix: &mut [u32],
    server_start: &[u32],
    client_start: &[u32],
    shape: &[u32],
    server_strides: &[usize],
    client_strides: &[usize],
    row_len: usize,
    segs: &mut Vec<(usize, usize, usize)>,
) {
    if dim + 1 == shape.len() {
        let mut s_idx = 0usize;
        let mut c_idx = 0usize;
        for i in 0..dim {
            s_idx += (server_start[i] + prefix[i]) as usize * server_strides[i];
            c_idx += (client_start[i] + prefix[i]) as usize * client_strides[i];
        }
        s_idx += server_start[dim] as usize * server_strides[dim];
        c_idx += client_start[dim] as usize * client_strides[dim];
        segs.push((s_idx, c_idx, row_len));
    } else {
        for v in 0..shape[dim] {
            prefix[dim] = v;
            gather_segments(dim+1, prefix, server_start, client_start, shape, server_strides, client_strides, row_len, segs);
        }
    }
}

fn compute_segments(
    server_start: &[u32],
    client_start: &[u32],
    shape: &[u32],
    server_shape: &[u32],
    client_shape: &[u32],
) -> Vec<(usize, usize, usize)> {
    let s_strides = strides(server_shape);
    let c_strides = strides(client_shape);
    let row_len = shape[shape.len()-1] as usize;
    let mut segs = Vec::new();
    let mut prefix = vec![0u32; shape.len()-1];
    if shape.len() == 1 {
        let s_idx = server_start[0] as usize;
        let c_idx = client_start[0] as usize;
        segs.push((s_idx, c_idx, row_len));
    } else {
        gather_segments(0, &mut prefix, server_start, client_start, shape, &s_strides, &c_strides, row_len, &mut segs);
    }
    segs
}

struct FilteredUpdate {
    shape: Vec<u32>,
    start: u32,
    values: Vec<f64>,
    name: Option<String>,
}

fn filter_updates(
    updates: &[Update],
    sub: &Subscription,
    state: &Shared,
    server_shape: &[u32],
) -> Vec<FilteredUpdate> {
    let mut out = Vec::new();
    for m in &sub.maps {
        let client_shape = match &m.target {
            Target::Region(_) => &sub.client_shape,
            Target::Named(_) => &m.shape,
        };
        let zero_vec;
        let client_start: &[u32] = match &m.target {
            Target::Region(cs) => cs.as_slice(),
            Target::Named(_) => {
                zero_vec = vec![0u32; m.shape.len()];
                &zero_vec
            }
        };

        let segs = compute_segments(&m.server_start, client_start, &m.shape, server_shape, client_shape);
        for u in updates {
            let u_start = u.start as usize;
            let u_end = u_start + u.len as usize;
            for (s_idx, c_idx, seg_len) in &segs {
                let seg_start = *s_idx;
                let seg_end = seg_start + *seg_len;
                let start = u_start.max(seg_start);
                let end = u_end.min(seg_end);
                if start < end {
                    let offset = start - seg_start;
                    let dst_start = c_idx + offset;
                    let len = end - start;
                    let mut vals = Vec::with_capacity(len);
                    for i in 0..len {
                        vals.push(state.get(start + i));
                    }
                    let name = match &m.target {
                        Target::Named(nm) => Some(nm.clone()),
                        _ => None,
                    };
                    out.push(FilteredUpdate {
                        shape: client_shape.to_vec(),
                        start: dst_start as u32,
                        values: vals,
                        name,
                    });
                }
            }
        }
    }
    out
}

pub async fn handle_peer(
    mut sock: TcpStream,
    state: Shared,
    named: Arc<HashMap<String, Shared>>,
    meta: Arc<Mutex<Vec<String>>>,
    notify: Arc<Notify>,
) -> Result<()> {
    let addr = sock.peer_addr().ok();
    println!("peer {:?} connected", addr);
    let local_shape = state.shape().to_vec();
    let len: usize = state.shape().iter().product();
    let named_map = named.clone();

    loop {
        let packet = match read_update_set(&mut sock).await? {
            Some(v) => v,
            None => break,
        };
        let (updates, metadata) = packet;
        for (shape, start_idx, vals, name) in updates {
            if shape == local_shape && name.is_none() {
                apply_update(&state, start_idx, &vals, len)?;
            } else if let Some(nm) = name {
                if let Some(dst) = named_map.get(&nm) {
                    let l: usize = dst.shape().iter().product();
                    apply_update(dst, start_idx, &vals, l)?;
                } else {
                    println!("unknown named slice {}", nm);
                }
            } else {
                println!("shape mismatch: recv {:?} local {:?}", shape, local_shape);
                continue;
            }
        }
        if let Some(m) = metadata {
            let mut q = meta.lock().unwrap();
            q.push(m);
        }
        notify.notify_waiters();
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
    let mut conns: Vec<(TcpStream, Subscription)> = Vec::new();
    loop {
        tokio::select! {
            res = lst.accept() => {
                let (mut sock, peer) = res?;
                println!("accepted connection from {}", peer);
                if let Some(sub) = read_subscription(&mut sock).await? {
                    let snap = snapshot_update(&state);
                    let server_shape: Vec<u32> = state.shape().iter().map(|&d| d as u32).collect();
                    let filtered = filter_updates(&[snap.clone()], &sub, &state, &server_shape);
                    let meta = pending_meta.lock().unwrap().clone();
                    let data = filtered_to_bytes(&filtered, &meta);
                    if sock.write_all(&data).await.is_ok() {
                        conns.push((sock, sub));
                    }
                }
            }
            res = rx.recv() => {
                let u = match res { Ok(v) => v, Err(_) => break };
                let mut alive = Vec::new();
                let server_shape: Vec<u32> = state.shape().iter().map(|&d| d as u32).collect();
                for (mut s, sub) in conns {
                    let filtered = filter_updates(&u.updates, &sub, &state, &server_shape);
                    if filtered.is_empty() && u.meta.is_none() {
                        alive.push((s, sub));
                        continue;
                    }
                    let data = filtered_to_bytes(&filtered, &u.meta);
                    match s.write_all(&data).await {
                        Ok(_) => alive.push((s, sub)),
                        Err(e) => println!("send failed: {}", e),
                    }
                }
                conns = alive;
            }
        }
    }
    Ok(())
}

pub async fn client(
    server: SocketAddr,
    state: Shared,
    named: Arc<HashMap<String, Shared>>,
    meta: Arc<Mutex<Vec<String>>>,
    sub: Subscription,
    notify: Arc<Notify>,
) -> Result<()> {
    let mut interval = time::interval(Duration::from_secs(1));
    loop {
        println!("connecting to server {}", server);
        match TcpStream::connect(server).await {
            Ok(mut sock) => {
                println!("connected to {}", server);
                let data = subscription_to_bytes(&sub);
                if sock.write_all(&data).await.is_err() {
                    continue;
                }
                let res = handle_peer(sock, state.clone(), named.clone(), meta.clone(), notify.clone()).await;
                if let Err(e) = res {
                    println!("connection error {}: {}", server, e);
                }
            }
            Err(e) => println!("failed to connect to {}: {}", server, e),
        }
        interval.tick().await;
    }
}

