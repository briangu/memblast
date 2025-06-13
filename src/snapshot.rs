use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug)]
pub struct UpdateDiff {
    pub start: usize,
    pub values: Vec<f64>,
}

#[derive(Clone)]
pub struct SnapshotManager {
    inner: Arc<Mutex<Inner>>, 
}

#[derive(Default)]
struct Inner {
    snapshots: BTreeMap<u64, Vec<f64>>, 
    diffs: BTreeMap<u64, Vec<UpdateDiff>>, 
}

impl SnapshotManager {
    pub fn new() -> Self {
        Self { inner: Arc::new(Mutex::new(Inner::default())) }
    }

    pub fn add_snapshot(&self, version: u64, snapshot: Vec<f64>) {
        let mut inner = self.inner.lock().unwrap();
        inner.snapshots.insert(version, snapshot);
    }

    pub fn add_updates(&self, version: u64, diffs: Vec<UpdateDiff>) {
        if diffs.is_empty() { return; }
        let mut inner = self.inner.lock().unwrap();
        inner.diffs.insert(version, diffs);
    }

    pub fn get(&self, version: u64) -> Option<Vec<f64>> {
        let inner = self.inner.lock().unwrap();
        let (&base_ver, snap) = inner.snapshots.range(..=version).next_back()?;
        let mut buf = snap.clone();
        if version > base_ver {
            for (_ver, diffs) in inner.diffs.range((base_ver + 1)..=version) {
                for d in diffs {
                    for (i, v) in d.values.iter().enumerate() {
                        let idx = d.start + i;
                        if idx < buf.len() {
                            buf[idx] = *v;
                        }
                    }
                }
            }
        }
        Some(buf)
    }
}
