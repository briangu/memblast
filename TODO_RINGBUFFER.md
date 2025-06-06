Below is a step-by-step "delta-feed refactor" checklist you can paste into TODO_RINGBUFFER.md (or feed to Codex one item at a time).
It converts the current full-snapshot protocol to an append-only ring buffer of deltas, still guarded by the odd/even seqlock so readers never see a torn message.

0 · High-level design
```
┌────────────┐                         ┌───────────┐
│  Writer    │  push {seq,len,bytes}   │  Clients  │
│ (ticker-p) │ ───────────────────────▶│ ring read │
└────────────┘                         └───────────┘
```
Ring buffer lives in one shared mmap (exported with PROT_READ to readers).

Each record header:

```rust
#[repr(C, packed)]
struct DeltaHdr {
    seq:   AtomicU64,   // even = stable; odd = being written
    len:   u32,         // bytes following header
    kind:  u8,          // 0 = data, 1 = checkpoint, etc.
    _pad:  [u8; 3],
}
// followed by `len` bytes payload (f64 array slice, JSON, whatever)
```
Writer: set seq |= 1, memcpy payload, fence, seq += 1.

Readers: poll hdr.seq & evenness; if seq > last_seen copy payload or interpret in-place.

1 · Constants & helpers
```
const PAGE: usize = 4096;

const RING_BYTES: usize = 16 * 1024 * 1024; // 16 MiB

const MAX_DELTA: usize = 64 * 1024; // 64 KiB per record

fn align_up(x, a) -> usize { (x + a - 1) & !(a - 1) }
```

2 · New module src/ring.rs
Map the ring

```rust
pub struct Ring {
    buf: MmapMut,
    head: AtomicU64,      // byte offset for next write
}

impl Ring {
    pub fn new(path: &Path) -> Result<Self> { … }   // tmpfs / shm file
}
```
Writer API

```rust
impl Ring {
    pub fn push(&self, kind: u8, payload: &[u8]) -> Result<u64> {
        assert!(payload.len() <= MAX_DELTA);
        let need = align_up(std::mem::size_of::<DeltaHdr>() + payload.len(), 8);
        let start = self.head.fetch_add(need as u64, Ordering::AcqRel) as usize % RING_BYTES;
        let hdr = unsafe { &mut *(self.buf.as_mut_ptr().add(start) as *mut DeltaHdr) };

        hdr.seq.fetch_add(1, Ordering::Release);              // odd
        hdr.len = payload.len() as u32;
        hdr.kind = kind;
        unsafe { std::ptr::copy_nonoverlapping(payload.as_ptr(), hdr as *mut _ as *mut u8).add(std::mem::size_of::<DeltaHdr>()), payload.len()) };
        std::sync::atomic::fence(Ordering::SeqCst);
        hdr.seq.fetch_add(1, Ordering::Release);              // even

        Ok(hdr.seq.load(Ordering::Acquire))
    }
}
```
Reader iterator

```rust
pub struct Cursor { pos: u64 }
impl Iterator for Cursor {
    type Item = Delta<'_>;
    fn next(&mut self) -> Option<Self::Item> { … }
}
```
3 · Wire protocol changes
 Deprecate Update { shape, arr } struct.

 Send raw ring data over TCP exactly once when a client connects:

Server sends checkpoint snapshot (kind = 1) so new client can reconstruct full state quickly.

Then it streams delta records (kind = 0) as they land.

4 · Integration with Python façade
Node.start
 Export ring fd to Python side via PyCapsule if you want zero-copy NumPy (np.memmap).

 Return both:

- the snapshot ndarray (checkpoint)
- an async iterator that yields deltas (Node.next_delta()).

WriteGuard.exit
 Instead of flush_now() copying the entire array, compute the updated slice offset & length and call ring.push(0, &bytes).

```rust
let start_byte = idx * std::mem::size_of::<f64>();
let end_byte = (idx + slice_len) * std::mem::size_of::<f64>();
ring.push(0, &state.mm.mm[start_byte..end_byte])?;
```
Read side example
```python
# client
snap = node.checkpoint()              # full array once
while True:
    kind, seq, delta = await node.next_delta()
    if kind == 0:  # data
        off, data = decode(delta)
        snap[off:off+len(data)] = data
```
5 · Back-pressure & wrap-around
 When head + need – tail > RING_BYTES:

Writer emits kind = 2 (wrap), sets head = 0.

Readers on catch-up detect kind = 2 and set pos = 0.

6 · Tests
`tests/ring_single.rs`

push 10 k records; iterate; assert seq monotonic, payload correct

`tests/ring_wrap.rs`

ring 1 MiB, push till wrap; confirm kind = 2 event resets cursor

`tests/ring_concurrent.rs`

spawn 1 writer + 4 reader threads; run 1 M pushes; checksum in readers == writer

7 · Migration plan
Phase	What to run	Outcome
A	Build ring.rs, standalone tests	proven data-structure
B	Swap flush() to call Ring::push	writer no longer broadcasts full array
C	Change broadcaster() to stream ring bytes	incremental updates on the wire
D	Remove old snapshot copy paths	done
