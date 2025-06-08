use anyhow::Result;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use crate::memory::Shared;
use crate::net::{self, UpdatePacket, Subscription};

pub async fn serve(
    addr: SocketAddr,
    rx: async_channel::Receiver<UpdatePacket>,
    state: Shared,
    pending_meta: Arc<Mutex<Option<String>>>,
) -> Result<()> {
    // Placeholder implementation using TCP transport.
    net::serve(addr, rx, state, pending_meta).await
}

pub async fn client(
    server: SocketAddr,
    state: Shared,
    named: Arc<HashMap<String, Shared>>,
    meta: Arc<Mutex<Vec<String>>>,
    sub: Subscription,
) -> Result<()> {
    // Placeholder implementation using TCP transport.
    net::client(server, state, named, meta, sub).await
}
