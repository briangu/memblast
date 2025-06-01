//! JSON/TCP network implementation for OpenRaft v1 with adapt-network-v1
use std::{collections::HashMap, net::SocketAddr, time::Duration};

use serde::{Deserialize, Serialize};
use serde_json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use openraft::error::{NetworkError, RPCError, RaftError};
use openraft::network::RPCOption;
use openraft::network::v1::{RaftNetwork, RaftNetworkFactory};
use openraft::raft::{AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse, VoteRequest, VoteResponse};
use openraft::RaftTypeConfig;

use crate::{TypeConfig};

/// Envelope for multiplexed RPCs over a single TCP connection.
/// Envelope for multiplexed RPCs over a single TCP connection.
#[derive(Serialize, Deserialize)]
pub struct RpcEnvelope<T> {
    pub uri: String,
    pub rpc: T,
}

/// Factory for building Raft network clients to peers.
#[derive(Clone)]
pub struct NetworkFactory {
    peers: HashMap<<TypeConfig as RaftTypeConfig>::NodeId, SocketAddr>,
}

impl NetworkFactory {
    pub fn new(peers: HashMap<<TypeConfig as RaftTypeConfig>::NodeId, SocketAddr>) -> Self {
        Self { peers }
    }
}

impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = Network;

    async fn new_client(
        &mut self,
        target: <TypeConfig as RaftTypeConfig>::NodeId,
        _node: &<TypeConfig as RaftTypeConfig>::Node,
    ) -> Self::Network {
        let addr = *self.peers.get(&target).expect("unknown raft peer id");
        Network { addr }
    }
}

/// Single-connection Raft network client for a specific peer.
pub struct Network {
    addr: SocketAddr,
}

impl Network {
    /// Send a multiplexed RPC and await its JSON-encoded response.
    async fn send_rpc<Req, Resp>(&mut self, uri: &str, rpc: Req) -> Result<Resp, RPCError<TypeConfig, RaftError<TypeConfig>>>
    where
        Req: Serialize,
        Resp: for<'de> Deserialize<'de> + Send,
    {
        let env = RpcEnvelope { uri: uri.to_string(), rpc };
        let buf = serde_json::to_vec(&env)
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        let mut stream = TcpStream::connect(self.addr)
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        let len = (buf.len() as u32).to_be_bytes();
        stream
            .write_all(&len)
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        stream
            .write_all(&buf)
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        let mut len_buf = [0u8; 4];
        stream
            .read_exact(&mut len_buf)
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        let resp_len = u32::from_be_bytes(len_buf) as usize;
        let mut resp_buf = vec![0u8; resp_len];
        stream
            .read_exact(&mut resp_buf)
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        let resp_env: RpcEnvelope<Resp> = serde_json::from_slice(&resp_buf)
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        Ok(resp_env.rpc)
    }
}

impl RaftNetwork<TypeConfig> for Network {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig, RaftError<TypeConfig>>> {
        self.send_rpc("append", rpc).await
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<InstallSnapshotResponse<TypeConfig>, RPCError<TypeConfig, RaftError<TypeConfig, crate::error::InstallSnapshotError>>> {
        self.send_rpc("snapshot", rpc).await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig, RaftError<TypeConfig>>> {
        self.send_rpc("vote", rpc).await
    }

    fn backoff(&self) -> openraft::network::Backoff {
        openraft::network::Backoff::new(std::iter::repeat(Duration::from_millis(500)))
    }
}