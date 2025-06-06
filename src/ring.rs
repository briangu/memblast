use std::sync::atomic::{AtomicU64, Ordering};
use std::fs::OpenOptions;
use std::path::Path;
use std::io::{Result as IoResult, Error, ErrorKind};
use memmap2::{MmapMut, MmapOptions};

const PAGE: usize = 4096;
/// Total bytes of the ring buffer.
pub const RING_BYTES: usize = 16 * 1024 * 1024; // 16 MiB
/// Maximum payload size for a single record.
pub const MAX_DELTA: usize = 64 * 1024; // 64 KiB

#[inline]
fn align_up(x: usize, a: usize) -> usize {
    (x + a - 1) & !(a - 1)
}

#[repr(C)]
struct DeltaHdr {
    seq: AtomicU64, // even = stable; odd = being written
    len: u32,       // payload length in bytes
    kind: u8,       // 0 = data, 1 = checkpoint, 2 = wrap
    _pad: [u8; 3],
}

pub struct Ring {
    buf: MmapMut,
    head: AtomicU64,
    seq: AtomicU64,
}

impl Ring {
    /// Create or open a ring buffer backed by the given path. The file will be
    /// truncated to `RING_BYTES`.
    pub fn new(path: &Path) -> IoResult<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)?;
        file.set_len(RING_BYTES as u64)?;
        let buf = unsafe { MmapOptions::new().len(RING_BYTES).map_mut(&file)? };
        Ok(Self { buf, head: AtomicU64::new(0), seq: AtomicU64::new(0) })
    }

    pub fn push(&self, kind: u8, payload: &[u8]) -> IoResult<u64> {
        if payload.len() > MAX_DELTA {
            return Err(Error::new(ErrorKind::InvalidInput, "payload too large"));
        }
        let need = align_up(std::mem::size_of::<DeltaHdr>() + payload.len(), 8);
        let start = self.head.fetch_add(need as u64, Ordering::AcqRel) as usize % RING_BYTES;
        if start + need > RING_BYTES {
            // not enough space at end, emit wrap marker and restart from 0
            let hdr = unsafe { &mut *(self.buf.as_ptr().add(start) as *mut DeltaHdr) };
            hdr.seq.store(1, Ordering::Release); // odd
            hdr.len = 0;
            hdr.kind = 2; // wrap
            std::sync::atomic::fence(Ordering::SeqCst);
            hdr.seq.store(2, Ordering::Release); // even
            self.head.store(need as u64, Ordering::Release);
            return Ok(2);
        }
        let hdr = unsafe { &mut *(self.buf.as_ptr().add(start) as *mut DeltaHdr) };
        let seq = self.seq.fetch_add(2, Ordering::AcqRel);
        hdr.seq.store(seq | 1, Ordering::Release); // mark odd
        hdr.len = payload.len() as u32;
        hdr.kind = kind;
        unsafe {
            let dst = self.buf.as_ptr().add(start + std::mem::size_of::<DeltaHdr>()) as *mut u8;
            std::ptr::copy_nonoverlapping(payload.as_ptr(), dst, payload.len());
        }
        std::sync::atomic::fence(Ordering::SeqCst);
        hdr.seq.store(seq + 2, Ordering::Release); // even
        Ok(seq + 2)
    }

    pub fn cursor(&self) -> Cursor<'_> {
        Cursor { ring: self, pos: 0 }
    }
}

pub struct Delta<'a> {
    pub seq: u64,
    pub kind: u8,
    pub payload: &'a [u8],
}

pub struct Cursor<'a> {
    ring: &'a Ring,
    pos: u64,
}

impl<'a> Iterator for Cursor<'a> {
    type Item = Delta<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let start = (self.pos as usize) % RING_BYTES;
        if start + std::mem::size_of::<DeltaHdr>() > RING_BYTES {
            self.pos = 0;
        }
        let hdr = unsafe { &* (self.ring.buf.as_ptr().add(start) as *const DeltaHdr) };
        let mut seq = hdr.seq.load(Ordering::Acquire);
        if seq % 2 == 1 { // writer in progress
            return None;
        }
        let len = hdr.len as usize;
        let kind = hdr.kind;
        if len == 0 && kind == 0 {
            return None;
        }
        let payload_start = start + std::mem::size_of::<DeltaHdr>();
        let payload = &self.ring.buf[payload_start..payload_start+len];
        self.pos += align_up(std::mem::size_of::<DeltaHdr>() + len, 8) as u64;
        seq = hdr.seq.load(Ordering::Acquire);
        if seq % 2 == 1 {
            return None;
        }
        Some(Delta { seq, kind, payload })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::sync::Arc;
    use tempfile;

    #[test]
    fn ring_single() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ring");
        let ring = Ring::new(&path).unwrap();
        for i in 0..10_000u32 {
            ring.push(0, &i.to_le_bytes()).unwrap();
        }
        let mut cur = ring.cursor();
        let mut seq_prev = 0;
        let mut count = 0;
        while let Some(delta) = cur.next() {
            let v = u32::from_le_bytes(delta.payload.try_into().unwrap());
            assert_eq!(v, count as u32);
            assert!(delta.seq > seq_prev);
            seq_prev = delta.seq;
            count += 1;
        }
        assert_eq!(count, 10_000);
    }

    #[test]
    fn ring_wrap() {
        const SMALL_BYTES: usize = 1 * 1024 * 1024; // 1 MiB
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ring2");
        // build smaller ring
        let file = OpenOptions::new().create(true).read(true).write(true).open(&path).unwrap();
        file.set_len(SMALL_BYTES as u64).unwrap();
        let buf = unsafe { MmapOptions::new().len(SMALL_BYTES).map_mut(&file).unwrap() };
        let ring = Ring { buf, head: AtomicU64::new(0), seq: AtomicU64::new(0) };
        let payload = [0u8; 128];
        let mut wrap_seen = false;
        for _ in 0..(SMALL_BYTES / payload.len() + 10) {
            let seq = ring.push(0, &payload).unwrap();
            if seq == 2 { wrap_seen = true; break; }
        }
        assert!(wrap_seen, "wrap marker not emitted");
    }

    #[test]
    fn ring_concurrent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ring3");
        let ring = Arc::new(Ring::new(&path).unwrap());
        let writer = ring.clone();
        let handle = thread::spawn(move || {
            for i in 0..100_000u32 {
                writer.push(0, &i.to_le_bytes()).unwrap();
            }
        });
        handle.join().unwrap();
        let mut readers = Vec::new();
        for _ in 0..4 {
            let r = ring.clone();
            readers.push(thread::spawn(move || {
                let mut cur = r.cursor();
                let mut sum = 0u64;
                while let Some(delta) = cur.next() {
                    let v = u32::from_le_bytes(delta.payload.try_into().unwrap());
                    sum += v as u64;
                }
                sum
            }));
        }
        let writer_sum: u64 = (0..100_000u64).sum();
        for h in readers {
            let sum = h.join().unwrap();
            assert_eq!(sum, writer_sum);
        }
    }
}

