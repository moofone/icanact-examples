#![allow(dead_code)]

use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::UnsafeCell,
    mem::MaybeUninit,
    sync::atomic::{AtomicU64, Ordering},
    thread,
    time::{Duration, Instant},
};

struct CountingAlloc;

static ALLOC_CALLS: AtomicU64 = AtomicU64::new(0);
static DEALLOC_CALLS: AtomicU64 = AtomicU64::new(0);
static REALLOC_CALLS: AtomicU64 = AtomicU64::new(0);
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);
static DEALLOC_BYTES: AtomicU64 = AtomicU64::new(0);
static REALLOC_BYTES: AtomicU64 = AtomicU64::new(0);

#[global_allocator]
static GLOBAL_ALLOC: CountingAlloc = CountingAlloc;

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        DEALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        DEALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        REALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        REALLOC_BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct AllocStats {
    pub alloc_calls: u64,
    pub dealloc_calls: u64,
    pub realloc_calls: u64,
    pub alloc_bytes: u64,
    pub dealloc_bytes: u64,
    pub realloc_bytes: u64,
}

pub fn alloc_reset() {
    ALLOC_CALLS.store(0, Ordering::Relaxed);
    DEALLOC_CALLS.store(0, Ordering::Relaxed);
    REALLOC_CALLS.store(0, Ordering::Relaxed);
    ALLOC_BYTES.store(0, Ordering::Relaxed);
    DEALLOC_BYTES.store(0, Ordering::Relaxed);
    REALLOC_BYTES.store(0, Ordering::Relaxed);
}

pub fn alloc_snapshot() -> AllocStats {
    AllocStats {
        alloc_calls: ALLOC_CALLS.load(Ordering::Relaxed),
        dealloc_calls: DEALLOC_CALLS.load(Ordering::Relaxed),
        realloc_calls: REALLOC_CALLS.load(Ordering::Relaxed),
        alloc_bytes: ALLOC_BYTES.load(Ordering::Relaxed),
        dealloc_bytes: DEALLOC_BYTES.load(Ordering::Relaxed),
        realloc_bytes: REALLOC_BYTES.load(Ordering::Relaxed),
    }
}

pub fn print_alloc_stats_row(name: &str, ops: u64, s: &AllocStats) {
    let ops_f = if ops == 0 { 1.0 } else { ops as f64 };
    let allocs_per_op = s.alloc_calls as f64 / ops_f;
    let bytes_per_op = s.alloc_bytes as f64 / ops_f;
    println!("== {name}_alloc ==");
    println!(
        "alloc_calls: {}  dealloc_calls: {}  realloc_calls: {}",
        s.alloc_calls, s.dealloc_calls, s.realloc_calls
    );
    println!(
        "alloc_bytes: {}  dealloc_bytes: {}  realloc_bytes: {}",
        s.alloc_bytes, s.dealloc_bytes, s.realloc_bytes
    );
    println!(
        "allocs_per_op: {:.6}  alloc_bytes_per_op: {:.3}",
        allocs_per_op, bytes_per_op
    );
    println!();
}

pub fn cpu_time() -> Duration {
    unsafe {
        let mut usage: libc::rusage = std::mem::zeroed();
        libc::getrusage(libc::RUSAGE_SELF, &mut usage);
        let user = Duration::new(
            usage.ru_utime.tv_sec as u64,
            (usage.ru_utime.tv_usec as u32) * 1_000,
        );
        let sys = Duration::new(
            usage.ru_stime.tv_sec as u64,
            (usage.ru_stime.tv_usec as u32) * 1_000,
        );
        user + sys
    }
}

pub fn fmt_duration(d: Duration) -> String {
    let secs = d.as_secs_f64();
    if secs < 1e-6 {
        format!("{:.3}ns", secs * 1e9)
    } else if secs < 1e-3 {
        format!("{:.3}us", secs * 1e6)
    } else if secs < 1.0 {
        format!("{:.3}ms", secs * 1e3)
    } else {
        format!("{:.3}s", secs)
    }
}

pub fn fmt_ns_as_us(ns: u64) -> String {
    format!("{:.3}us", (ns as f64) / 1_000.0)
}

#[inline(always)]
pub fn spin_then_yield(spins: &mut u32) {
    if *spins < 64 {
        *spins += 1;
        std::hint::spin_loop();
    } else {
        *spins = 0;
        thread::yield_now();
    }
}

pub fn percentile_sorted(v: &[u64], p: f64) -> u64 {
    if v.is_empty() {
        return 0;
    }
    let n = v.len();
    let idx = ((n as f64 - 1.0) * p).round() as usize;
    v[idx.min(n - 1)]
}

#[derive(Debug, Clone)]
pub struct LatencyStats {
    pub p50: Duration,
    pub p99: Duration,
    pub p999: Duration,
    pub max: Duration,
    pub samples: usize,
}

pub fn latency_stats_from_nanos(mut samples: Vec<u64>) -> LatencyStats {
    samples.sort_unstable();
    let p50 = percentile_sorted(&samples, 0.50);
    let p99 = percentile_sorted(&samples, 0.99);
    let p999 = percentile_sorted(&samples, 0.999);
    let max = *samples.last().unwrap_or(&0);
    LatencyStats {
        p50: Duration::from_nanos(p50),
        p99: Duration::from_nanos(p99),
        p999: Duration::from_nanos(p999),
        max: Duration::from_nanos(max),
        samples: samples.len(),
    }
}

pub fn print_latency_row(name: &str, s: &LatencyStats) {
    println!("== {name} ==");
    println!("samples: {}", s.samples);
    println!(
        "p50: {}  p99: {}  p999: {}  max: {}",
        fmt_duration(s.p50),
        fmt_duration(s.p99),
        fmt_duration(s.p999),
        fmt_duration(s.max),
    );
    println!();
}

pub const LAT_SAMPLE_FLAG: u64 = 1u64 << 63;

#[inline(always)]
pub fn now_ns(base: Instant) -> u64 {
    base.elapsed().as_nanos() as u64
}

#[inline(always)]
pub fn encode_sample_t0(t0_ns: u64) -> u64 {
    LAT_SAMPLE_FLAG | t0_ns
}

#[inline(always)]
pub fn decode_sample_t0(v: u64) -> Option<u64> {
    if (v & LAT_SAMPLE_FLAG) != 0 {
        Some(v & !LAT_SAMPLE_FLAG)
    } else {
        None
    }
}

pub struct LatencyBuf {
    buf: Box<[UnsafeCell<MaybeUninit<u64>>]>,
    idx: std::sync::atomic::AtomicUsize,
}

// Single consumer writes; main thread reads after join.
unsafe impl Sync for LatencyBuf {}

impl LatencyBuf {
    pub fn new(cap: usize) -> Self {
        let mut v = Vec::with_capacity(cap);
        for _ in 0..cap {
            v.push(UnsafeCell::new(MaybeUninit::uninit()));
        }
        Self {
            buf: v.into_boxed_slice(),
            idx: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    #[inline(always)]
    pub fn reset(&self) {
        self.idx.store(0, std::sync::atomic::Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn record(&self, dt_ns: u64) {
        let i = self.idx.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if i < self.buf.len() {
            unsafe {
                (*self.buf[i].get()).write(dt_ns);
            }
        }
    }

    pub fn collect(&self) -> Vec<u64> {
        let n = self
            .idx
            .load(std::sync::atomic::Ordering::Relaxed)
            .min(self.buf.len());
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            let v = unsafe { (*self.buf[i].get()).assume_init() };
            out.push(v);
        }
        out
    }
}

pub struct RateLimiter {
    base: Instant,
    step_ns: u64,
    next_due_ns: u64,
}

impl RateLimiter {
    pub fn new(base: Instant, rate: u64) -> Self {
        let step_ns = if rate > 0 { 1_000_000_000u64 / rate } else { 0 };
        Self {
            base,
            step_ns,
            next_due_ns: 0,
        }
    }

    #[inline(always)]
    pub fn wait(&mut self) {
        if self.step_ns == 0 {
            return;
        }
        self.next_due_ns = self.next_due_ns.wrapping_add(self.step_ns);
        loop {
            let now = now_ns(self.base);
            if now >= self.next_due_ns {
                break;
            }
            // Avoid burning 100% CPU at low offered loads.
            if (self.next_due_ns - now) > 50_000 {
                thread::yield_now();
            } else {
                std::hint::spin_loop();
            }
        }
    }
}
