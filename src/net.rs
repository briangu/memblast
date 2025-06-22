use anyhow::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{self, Duration};
use std::collections::HashMap;
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use serde::{Serialize, Deserialize};
use bincode;

use crate::memory::Shared;


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Update {
    pub start: u32,
    pub len: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdatePacket {
    pub updates: Vec<Update>,
    pub meta: Option<String>,
    pub version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Target {
    Region(Vec<u32>),
    Named(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Mapping {
    pub server_start: Vec<u32>,
    pub shape: Vec<u32>,
    pub target: Target,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Subscription {
    pub name: String,
    pub client_shape: Vec<u32>,
    pub maps: Vec<Mapping>,
    pub hash_check: bool,
}




#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct WireUpdate {
    version: u64,
    updates: Vec<FilteredUpdate>,
    meta: Option<String>,
    hash: Option<[u8; 32]>,
}

async fn send_message<T: Serialize>(sock: &mut TcpStream, msg: &T) -> Result<()> {
    let data = bincode::serialize(msg)?;
    sock.write_all(&(data.len() as u32).to_le_bytes()).await?;
    sock.write_all(&data).await?;
    Ok(())
}

async fn recv_message<T: for<'de> Deserialize<'de>>(sock: &mut TcpStream) -> Result<Option<T>> {
    let mut len_buf = [0u8; 4];
    if !read_exact_checked(sock, &mut len_buf).await? {
        return Ok(None);
    }
    let len = u32::from_le_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    if !read_exact_checked(sock, &mut buf).await? {
        return Ok(None);
    }
    let msg = bincode::deserialize(&buf)?;
    Ok(Some(msg))
}

async fn send_subscription(sock: &mut TcpStream, sub: &Subscription) -> Result<()> {
    send_message(sock, sub).await
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
    recv_message(sock).await
}


async fn read_update_set(sock: &mut TcpStream) -> Result<Option<WireUpdate>> {
    recv_message(sock).await
}

fn connection_closed(sock: &mut TcpStream) -> bool {
    let mut buf = [0u8; 1];
    match sock.try_read(&mut buf) {
        Ok(0) => true,
        Ok(_) => false,
        Err(ref e) if e.kind() == ErrorKind::WouldBlock => false,
        Err(_) => true,
    }
}

fn apply_update_values(state: &Shared, start_idx: usize, values: &[f64]) -> Result<()> {
    let len: usize = state.shape().iter().product();
    state.start_write()?;
    for (i, &v) in values.iter().enumerate() {
        let idx = start_idx + i;
        if idx < len {
            let base = state.mm.mm.as_ptr() as *mut f64;
            unsafe { *base.add(idx) = v; }
        }
    }
    state.end_write()?;
    Ok(())
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

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
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
    versions: Arc<Mutex<HashMap<String, u64>>>,
    peer_id: String,
    hash_check: bool,
) -> Result<()> {
    let addr = sock.peer_addr().ok();
    println!("peer {:?} connected", addr);
    let local_shape: Vec<u32> = state.shape().iter().map(|&d| d as u32).collect();
    let named_map = named.clone();

    loop {
        let packet = match read_update_set(&mut sock).await? {
            Some(v) => v,
            None => break,
        };
        {
            let mut map = versions.lock().unwrap();
            map.insert(peer_id.clone(), packet.version);
        }
        for upd in packet.updates {
            let target = if upd.shape == local_shape && upd.name.is_none() {
                Some(state.clone())
            } else if let Some(ref nm) = upd.name {
                named_map.get(nm).cloned()
            } else {
                println!("shape mismatch: recv {:?} local {:?}", upd.shape, local_shape);
                None
            };
            if let Some(dst) = target {
                apply_update_values(&dst, upd.start as usize, &upd.values)?;
            }
        }
        if let Some(m) = packet.meta {
            let mut q = meta.lock().unwrap();
            q.push(m);
        }
        if let (true, Some(h)) = (hash_check, packet.hash) {
            let local = state.snapshot_hash();
            if h != local {
                println!("hash mismatch from {:?}", addr);
            }
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
    let mut conns: Vec<(TcpStream, Subscription)> = Vec::new();
    loop {
        tokio::select! {
            res = lst.accept() => {
                let (mut sock, peer) = res?;
                if let Some(sub) = read_subscription(&mut sock).await? {
                    println!("accepted connection from {} named {}", peer, sub.name);
                    let snap = snapshot_update(&state);
                    let server_shape: Vec<u32> = state.shape().iter().map(|&d| d as u32).collect();
                    let filtered = filter_updates(&[snap.clone()], &sub, &state, &server_shape);
                    let meta = pending_meta.lock().unwrap().clone();
                    let snap_hash = if sub.hash_check { Some(state.snapshot_hash()) } else { None };
                    let wire = WireUpdate { version: 0, updates: filtered, meta, hash: snap_hash };
                    if send_message(&mut sock, &wire).await.is_ok() {
                        conns.push((sock, sub));
                    }
                }
            }
            res = rx.recv() => {
                let u = match res { Ok(v) => v, Err(_) => break };
                let mut alive = Vec::new();
                let server_shape: Vec<u32> = state.shape().iter().map(|&d| d as u32).collect();
                let hash_val = if conns.iter().any(|(_, sub)| sub.hash_check) {
                    Some(state.snapshot_hash())
                } else { None };
                for (mut s, sub) in conns {
                    if connection_closed(&mut s) {
                        println!("peer disconnected");
                        // explicitly drop the closed connection
                        drop(s);
                        continue;
                    }
                    let filtered = filter_updates(&u.updates, &sub, &state, &server_shape);
                    if filtered.is_empty() && u.meta.is_none() {
                        alive.push((s, sub));
                        continue;
                    }
                    let hash_ref = if sub.hash_check { hash_val.as_ref() } else { None };
                    let wire = WireUpdate { version: u.version, updates: filtered, meta: u.meta.clone(), hash: hash_ref.cloned() };
                    match send_message(&mut s, &wire).await {
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
    versions: Arc<Mutex<HashMap<String, u64>>>,
    sub: Subscription,
) -> Result<()> {
    let mut interval = time::interval(Duration::from_secs(1));
    loop {
        println!("connecting to server {}", server);
        match TcpStream::connect(server).await {
            Ok(mut sock) => {
                println!("connected to {}", server);
                if send_subscription(&mut sock, &sub).await.is_err() {
                    continue;
                }
                let res = handle_peer(
                    sock,
                    state.clone(),
                    named.clone(),
                    meta.clone(),
                    versions.clone(),
                    server.to_string(),
                    sub.hash_check,
                ).await;
                if let Err(e) = res {
                    println!("connection error {}: {}", server, e);
                }
            }
            Err(e) => println!("failed to connect to {}: {}", server, e),
        }
        interval.tick().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::{MmapBuf, Shared};
    use tokio::net::{TcpListener, TcpStream};
    use std::sync::{Arc, Mutex};
    use std::collections::HashMap;

    fn make_shared(shape: Vec<usize>) -> Shared {
        Shared::new(MmapBuf::new(shape).unwrap())
    }

    fn write_values(mem: &Shared, vals: &[f64]) {
        mem.start_write().unwrap();
        let ptr = mem.mm.mm.as_ptr() as *mut f64;
        for (i, &v) in vals.iter().enumerate() {
            unsafe { *ptr.add(i) = v; }
        }
        mem.end_write().unwrap();
    }

    #[tokio::test]
    async fn wireupdate_roundtrip() {
        let upd = FilteredUpdate {
            shape: vec![2],
            start: 1,
            values: vec![1.0, 2.0],
            name: Some("x".to_string()),
        };
        let wire = WireUpdate {
            version: 7,
            updates: vec![upd.clone()],
            meta: Some("m".into()),
            hash: Some([1u8; 32]),
        };
        let bytes = bincode::serialize(&wire).unwrap();
        let de: WireUpdate = bincode::deserialize(&bytes).unwrap();
        assert_eq!(de, wire);
    }

    #[tokio::test]
    async fn subscription_flow() {
        let server_mem = make_shared(vec![2]);
        write_values(&server_mem, &[1.2, 3.4]);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_task = tokio::spawn({
            let server_mem = server_mem.clone();
            async move {
                let (mut sock, _) = listener.accept().await.unwrap();
                if let Some(sub) = read_subscription(&mut sock).await.unwrap() {
                    let snap = snapshot_update(&server_mem);
                    let server_shape: Vec<u32> = server_mem
                        .shape()
                        .iter()
                        .map(|&d| d as u32)
                        .collect();
                    let filtered = filter_updates(&[snap], &sub, &server_mem, &server_shape);
                    let wire = WireUpdate { version: 0, updates: filtered, meta: None, hash: None };
                    send_message(&mut sock, &wire).await.unwrap();
                }
            }
        });

        let client_mem = make_shared(vec![2]);
        let versions = Arc::new(Mutex::new(HashMap::new()));
        let meta = Arc::new(Mutex::new(Vec::new()));
        let named = Arc::new(HashMap::new());

        let mut sock = TcpStream::connect(addr).await.unwrap();
        let sub = Subscription {
            name: "c".into(),
            client_shape: vec![2],
            maps: vec![Mapping {
                server_start: vec![0],
                shape: vec![2],
                target: Target::Region(vec![0]),
            }],
            hash_check: false,
        };
        send_subscription(&mut sock, &sub).await.unwrap();
        handle_peer(
            sock,
            client_mem.clone(),
            named,
            meta,
            versions.clone(),
            "srv".into(),
            false,
        )
        .await
        .unwrap();

        server_task.await.unwrap();

        assert!((client_mem.get(0) - 1.2).abs() < 1e-12);
        assert!((client_mem.get(1) - 3.4).abs() < 1e-12);
        assert_eq!(versions.lock().unwrap().get("srv"), Some(&0u64));
    }
}

