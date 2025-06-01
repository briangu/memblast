//! In-memory Raft storage that replicates a shared memory map snapshot using OpenRaft.

use std::{io::Cursor, sync::{Arc, atomic::{AtomicU64, Ordering}}};
use openraft::{Entry, EntryPayload, LogId, StorageError, OptionalSend};
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine, Snapshot, SnapshotMeta};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::{Shared, TypeConfig};
use openraft::RaftTypeConfig;
use openraft_memstore::{new_mem_store as new_log_store, MemLogStore};

/// Application request: full snapshot bytes of the NumPy array.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ClientRequest {
    pub data: Vec<u8>,
}

/// Application response: empty marker.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ClientResponse;

/// In-memory snapshot representation.
#[derive(Debug, Clone)]
pub struct StoredSnapshot {
    pub meta: SnapshotMeta<TypeConfig>,
    pub data: Vec<u8>,
}

/// State machine data.
#[derive(Debug, Clone, Default)]
pub struct StateMachineData {
    pub last_applied_log: Option<LogId<TypeConfig>>,
    pub last_membership: openraft::StoredMembership<TypeConfig>,
    pub data: Vec<u8>,
}

/// Raft state machine store for applying committed entries and snapshots.
#[derive(Debug)]
pub struct StateMachineStore {
    state_machine: RwLock<StateMachineData>,
    snapshot_idx: AtomicU64,
    current_snapshot: RwLock<Option<StoredSnapshot>>,
    shared: Shared,
}

impl StateMachineStore {
    pub fn new(shared: Shared) -> Self {
        Self {
            state_machine: RwLock::new(StateMachineData::default()),
            snapshot_idx: AtomicU64::new(0),
            current_snapshot: RwLock::new(None),
            shared,
        }
    }
}

/// Create a new log store and state machine store pair.
pub fn new_mem_store(shared: Shared) -> (Arc<MemLogStore>, Arc<StateMachineStore>) {
    let (log_store, _mem_sm) = new_log_store();
    (log_store, Arc::new(StateMachineStore::new(shared)))
}

/// Stub implementations of RaftLogStorage and RaftLogReader for our TypeConfig,
/// delegating to the in-memory log store from openraft-memstore crate.
use openraft::storage::{RaftLogReader, RaftLogStorage, LogState, IOFlushed};
use std::{fmt::Debug, ops::RangeBounds};

#[openraft::add_async_trait]
impl RaftLogReader<TypeConfig> for Arc<MemLogStore> {
    async fn try_get_log_entries<RB>(&mut self, _range: RB) -> Result<Vec<Entry<TypeConfig>>, StorageError<TypeConfig>>
    where
        RB: RangeBounds<u64> + Clone + Debug + OptionalSend,
    {
        Ok(vec![])
    }

    async fn read_vote(&mut self) -> Result<Option<openraft::Vote<TypeConfig>>, StorageError<TypeConfig>> {
        Ok(None)
    }
}

#[openraft::add_async_trait]
impl RaftLogStorage<TypeConfig> for Arc<MemLogStore> {
    type LogReader = Arc<MemLogStore>;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<TypeConfig>> {
        Ok(LogState { last_purged_log_id: None, last_log_id: None })
    }

    async fn save_committed(&mut self, _committed: Option<LogId<TypeConfig>>) -> Result<(), StorageError<TypeConfig>> {
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<TypeConfig>>, StorageError<TypeConfig>> {
        Ok(None)
    }

    async fn save_vote(&mut self, _vote: &openraft::Vote<TypeConfig>) -> Result<(), StorageError<TypeConfig>> {
        Ok(())
    }

    async fn append<I>(&mut self, _entries: I, _callback: IOFlushed<TypeConfig>) -> Result<(), StorageError<TypeConfig>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
    {
        Ok(())
    }

    async fn truncate(&mut self, _log_id: LogId<TypeConfig>) -> Result<(), StorageError<TypeConfig>> {
        Ok(())
    }

    async fn purge(&mut self, _log_id: LogId<TypeConfig>) -> Result<(), StorageError<TypeConfig>> {
        Ok(())
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }
}

impl RaftSnapshotBuilder<TypeConfig> for Arc<StateMachineStore> {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<TypeConfig>> {
        let (data, last_applied, last_membership) = {
            let sm = self.state_machine.read().await;
            (sm.data.clone(), sm.last_applied_log, sm.last_membership.clone())
        };
        let idx = self.snapshot_idx.fetch_add(1, Ordering::Relaxed) + 1;
        let snapshot_id = if let Some(last) = last_applied {
            format!("{}-{}-{}", last.committed_leader_id(), last.index(), idx)
        } else {
            format!("--{}", idx)
        };
        let meta = SnapshotMeta { last_log_id: last_applied, last_membership, snapshot_id };
        let snap = StoredSnapshot { meta: meta.clone(), data: data.clone() };
        *self.current_snapshot.write().await = Some(snap);
        Ok(Snapshot { meta, snapshot: Cursor::new(data) })
    }
}

impl RaftStateMachine<TypeConfig> for Arc<StateMachineStore> {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<TypeConfig>>, openraft::StoredMembership<TypeConfig>), StorageError<TypeConfig>> {
        let sm = self.state_machine.read().await;
        Ok((sm.last_applied_log, sm.last_membership.clone()))
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn apply<I>(&mut self, entries: I) -> Result<Vec<ClientResponse>, StorageError<TypeConfig>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut res = Vec::new();
        for entry in entries {
            let mut sm = self.state_machine.write().await;
            sm.last_applied_log = Some(entry.log_id);
            match entry.payload {
                EntryPayload::Blank => res.push(ClientResponse),
                EntryPayload::Normal(req) => {
                    sm.data = req.data.clone();
                    let dst = unsafe {
                        std::slice::from_raw_parts_mut(
                            self.shared.0.mm.as_ptr() as *mut u8,
                            sm.data.len(),
                        )
                    };
                    dst.copy_from_slice(&sm.data);
                    res.push(ClientResponse);
                }
                EntryPayload::Membership(mem) => {
                    sm.last_membership = openraft::StoredMembership::new(Some(entry.log_id), mem.clone());
                    res.push(ClientResponse);
                }
            }
        }
        Ok(res)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn begin_receiving_snapshot(&mut self) -> Result<<TypeConfig as RaftTypeConfig>::SnapshotData, StorageError<TypeConfig>> {
        Ok(Cursor::new(Vec::new()))
    }

    #[tracing::instrument(level = "trace", skip(self, snapshot))]
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<TypeConfig>,
        snapshot: <TypeConfig as RaftTypeConfig>::SnapshotData,
    ) -> Result<(), StorageError<TypeConfig>> {
        let data = snapshot.into_inner();
        let snap = StoredSnapshot { meta: meta.clone(), data: data.clone() };
        {
            let mut sm = self.state_machine.write().await;
            sm.last_applied_log = meta.last_log_id;
            sm.last_membership = meta.last_membership.clone();
            sm.data = data;
        }
        *self.current_snapshot.write().await = Some(snap);
        let sm = self.state_machine.read().await;
        let dst = unsafe {
            std::slice::from_raw_parts_mut(
                self.shared.0.mm.as_ptr() as *mut u8,
                sm.data.len(),
            )
        };
        dst.copy_from_slice(&sm.data);
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<TypeConfig>>, StorageError<TypeConfig>> {
        if let Some(snap) = &*self.current_snapshot.read().await {
            let data = snap.data.clone();
            Ok(Some(Snapshot { meta: snap.meta.clone(), snapshot: Cursor::new(data) }))
        } else {
            Ok(None)
        }
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }
}
