use anyhow::{Result, bail};
use std::net::SocketAddr;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, atomic::{AtomicU64, Ordering}};
use async_rdma::RdmaBuilder;

use crate::net::{UpdatePacket, Subscription};
use crate::memory::Shared;

pub async fn serve(
    _addr: SocketAddr,
    _rx: async_channel::Receiver<UpdatePacket>,
    _state: Shared,
    _pending_meta: Arc<Mutex<Option<String>>>,
) -> Result<()> {
    // placeholder to ensure crate is linked and functions match signature
    let _ = RdmaBuilder::default();
    bail!("rdma backend not implemented yet");
}

pub async fn client(
    _server: SocketAddr,
    _state: Shared,
    _named: Arc<HashMap<String, Shared>>,
    _meta: Arc<Mutex<Vec<String>>>,
    _version: Arc<AtomicU64>,
    _sub: Subscription,
) -> Result<()> {
    let _ = RdmaBuilder::default();
    bail!("rdma backend not implemented yet");
}
