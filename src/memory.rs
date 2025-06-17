use anyhow::Result;
use libc::mprotect;
use libc::{PROT_READ, PROT_WRITE};
use memmap2::{MmapMut, MmapOptions};
use std::os::raw::c_void;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::atomic::fence;
use std::sync::Arc;
use sha2::{Digest, Sha256};

pub struct MmapBuf {
    pub mm: MmapMut,
    shape: Vec<usize>,
}

impl MmapBuf {
    pub fn new(shape: Vec<usize>) -> Result<Self> {
        let len: usize = shape.iter().product();
        let layout = len * std::mem::size_of::<f64>();
        let mm = MmapOptions::new().len(layout).map_anon()?;
        Ok(Self { mm, shape })
    }

    pub fn ptr(&self) -> *mut c_void {
        self.mm.as_ptr() as *mut _
    }

    pub fn byte_len(&self) -> usize {
        self.mm.len()
    }

    pub fn shape(&self) -> &[usize] {
        &self.shape
    }
}

#[derive(Clone)]
pub struct Shared {
    pub mm: Arc<MmapBuf>,
    pub seq: Arc<AtomicUsize>,
}

impl Shared {
    pub fn new(mm: MmapBuf) -> Self {
        Self { mm: Arc::new(mm), seq: Arc::new(AtomicUsize::new(0)) }
    }

    pub fn get(&self, idx: usize) -> f64 {
        unsafe {
            let base = self.mm.mm.as_ptr() as *const f64;
            *base.add(idx)
        }
    }

    pub fn shape(&self) -> &[usize] {
        self.mm.shape()
    }

    pub fn protect(&self, prot: i32) -> Result<()> {
        let len = self.mm.byte_len();
        let ret = unsafe { mprotect(self.mm.mm.as_ptr() as *mut _, len, prot) };
        if ret != 0 {
            Err(anyhow::anyhow!("mprotect failed"))
        } else {
            Ok(())
        }
    }

    pub fn start_write(&self) -> Result<()> {
        loop {
            let cur = self.seq.load(Ordering::Acquire);
            if cur % 2 == 1 {
                std::hint::spin_loop();
                continue;
            }
            match self.seq.compare_exchange(cur, cur + 1, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => break,
                Err(_) => {
                    std::hint::spin_loop();
                }
            }
        }
        self.protect(PROT_READ | PROT_WRITE)
    }

    pub fn end_write(&self) -> Result<()> {
        self.protect(PROT_READ)?;
        self.seq.fetch_add(1, Ordering::Release);
        Ok(())
    }

    pub fn read_snapshot(&self, buf: &mut [f64]) {
        loop {
            let start = self.seq.load(Ordering::Acquire);
            if start % 2 == 1 { continue; }
            fence(Ordering::Acquire);
            unsafe {
                std::ptr::copy_nonoverlapping(
                    self.mm.mm.as_ptr() as *const f64,
                    buf.as_mut_ptr(),
                    buf.len(),
                );
            }
            fence(Ordering::Acquire);
            if self.seq.load(Ordering::Relaxed) == start { break; }
        }
    }

    pub fn snapshot_hash(&self) -> [u8; 32] {
        let len: usize = self.shape().iter().product();
        let mut buf = vec![0f64; len];
        self.read_snapshot(&mut buf);
        let bytes = unsafe {
            std::slice::from_raw_parts(buf.as_ptr() as *const u8, len * 8)
        };
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        let result = hasher.finalize();
        let mut out = [0u8; 32];
        out.copy_from_slice(&result);
        out
    }
}

