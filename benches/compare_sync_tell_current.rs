// Sync tell comparison notes:
//
// - `actor_framework_sync` is the current public `SyncActorRef::tell(...)` path.
// - `actor_framework_sync_snapshot` is a lower-level mailbox snapshot/baseline path kept only to
//   show the remaining distance from the public API to the direct-mailbox floor.
// - This bench now uses randomized pre-generated payloads plus a checksum in every sink actor so
//   repeated constant-message optimization is not the dominant result driver.
// - `AF_CMP_SYNC_MAILBOX_CAP` materially changes interpretation. With a very large capacity
//   (for example `1000000`), the actor-framework public tell path behaves like a near-unbounded
//   steady-state enqueue and becomes much more representative of pure send overhead than of
//   bounded-admission/backoff behavior.
// - Actix here still uses `do_send`, so this is best read as a convenience-API comparison, not a
//   strict bounded-admission equivalence test.

use actix::{
    Actor as ActixActor, Handler as ActixHandler, Message as ActixMessage, SyncArbiter,
    SyncContext, System,
};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use icanact_core::{local_sync as sync, local_sync::SyncActor};
use rand::rngs::SmallRng;
use rand::{RngCore, SeedableRng};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug)]
struct Config {
    ops: u64,
    mailbox_cap: usize,
    actix_sync_threads: usize,
    producers: usize,
    sample_size: usize,
    warmup_ms: u64,
    measurement_ms: u64,
}

fn parse_u64_env(key: &str, default: u64) -> u64 {
    match std::env::var(key) {
        Ok(v) => v.parse::<u64>().unwrap_or(default),
        Err(_) => default,
    }
}

fn parse_usize_env(key: &str, default: usize) -> usize {
    match std::env::var(key) {
        Ok(v) => v.parse::<usize>().unwrap_or(default),
        Err(_) => default,
    }
}

impl Config {
    fn from_env() -> Self {
        Self {
            ops: parse_u64_env("AF_CMP_TRY_TELL_OPS", 300_000),
            mailbox_cap: parse_usize_env("AF_CMP_SYNC_MAILBOX_CAP", 300_000).max(1),
            actix_sync_threads: parse_usize_env("AF_CMP_ACTIX_SYNC_THREADS", 1).max(1),
            producers: parse_usize_env("AF_CMP_TELL_PRODUCERS", 1).max(1),
            sample_size: parse_usize_env("AF_CMP_SAMPLE_SIZE", 10).clamp(10, 100),
            warmup_ms: parse_u64_env("AF_CMP_WARMUP_MS", 150),
            measurement_ms: parse_u64_env("AF_CMP_MEASUREMENT_MS", 700),
        }
    }
}

fn split_ops(total: u64, producers: usize) -> Vec<u64> {
    let base = total / producers as u64;
    let extra = total % producers as u64;
    (0..producers)
        .map(|idx| base + u64::from((idx as u64) < extra))
        .collect()
}

fn make_values(n: u64, seed: u64) -> Vec<u64> {
    let mut rng = SmallRng::seed_from_u64(seed);
    let mut out = Vec::with_capacity(n as usize);
    for _ in 0..n {
        out.push(rng.next_u64());
    }
    out
}

#[inline(always)]
fn spin_then_yield(spins: &mut u32) {
    if *spins < 64 {
        *spins += 1;
        std::hint::spin_loop();
    } else {
        *spins = 0;
        std::thread::yield_now();
    }
}

struct AfSyncTryTellActor {
    processed: Arc<AtomicU64>,
    checksum: Arc<AtomicU64>,
}

impl SyncActor for AfSyncTryTellActor {
    type Contract = sync::contract::TellOnly;
    type Tell = u64;
    type Ask = ();
    type Reply = ();

    #[inline(always)]
    fn handle_tell(&mut self, msg: Self::Tell) {
        self.processed.fetch_add(1, Ordering::Relaxed);
        self.checksum.fetch_add(msg, Ordering::Relaxed);
    }
}

struct ActixSyncTryTellActor {
    processed: Arc<AtomicU64>,
    checksum: Arc<AtomicU64>,
}

#[derive(ActixMessage)]
#[rtype(result = "()")]
struct ActixTryTellMsg(u64);

impl ActixActor for ActixSyncTryTellActor {
    type Context = SyncContext<Self>;
}

impl ActixHandler<ActixTryTellMsg> for ActixSyncTryTellActor {
    type Result = ();

    #[inline(always)]
    fn handle(&mut self, msg: ActixTryTellMsg, _ctx: &mut Self::Context) -> Self::Result {
        self.processed.fetch_add(1, Ordering::Relaxed);
        self.checksum.fetch_add(msg.0, Ordering::Relaxed);
    }
}

fn run_af_sync_tell_snapshot(cfg: Config, iters: u64) -> Duration {
    let processed = Arc::new(AtomicU64::new(0));
    let checksum = Arc::new(AtomicU64::new(0));
    let warmup_vals = Arc::new(make_values(cfg.ops.min(10_000), 0x11));
    let bench_vals = Arc::new(make_values(cfg.ops, 0x22));
    let (actor_ref, handle) = sync::spawn_with_opts(
        AfSyncTryTellActor {
            processed: Arc::clone(&processed),
            checksum: Arc::clone(&checksum),
        },
        sync::SpawnOpts {
            mailbox_capacity: cfg.mailbox_cap,
            ..sync::SpawnOpts::default()
        },
    );
    handle.wait_for_startup();
    let addr = actor_ref
        .snapshot()
        .expect("sync actor ref should expose mailbox snapshot");

    let warmup = warmup_vals.len() as u64;
    for &v in warmup_vals.iter() {
        let mut spins = 0u32;
        while !addr.tell(v) {
            spin_then_yield(&mut spins);
        }
    }
    while processed.load(Ordering::Relaxed) < warmup {
        let mut spins = 0u32;
        spin_then_yield(&mut spins);
    }

    let t0 = Instant::now();
    let per_producer = split_ops(cfg.ops, cfg.producers);
    for _ in 0..iters {
        processed.store(0, Ordering::Relaxed);
        checksum.store(0, Ordering::Relaxed);
        let mut joins = Vec::with_capacity(cfg.producers);
        for ops in per_producer.iter().copied() {
            let addr = addr.clone();
            let bench_vals = Arc::clone(&bench_vals);
            joins.push(thread::spawn(move || {
                for idx in 0..ops as usize {
                    let mut spins = 0u32;
                    let v = bench_vals[idx];
                    while !addr.tell(v) {
                        spin_then_yield(&mut spins);
                    }
                }
            }));
        }
        for join in joins {
            join.join().expect("actor-framework sync producer panicked");
        }
        while processed.load(Ordering::Relaxed) < cfg.ops {
            let mut spins = 0u32;
            spin_then_yield(&mut spins);
        }
        std::hint::black_box(checksum.load(Ordering::Relaxed));
    }
    let elapsed = t0.elapsed();
    handle.shutdown();
    elapsed
}

fn run_af_sync_tell_actor_ref(cfg: Config, iters: u64) -> Duration {
    let processed = Arc::new(AtomicU64::new(0));
    let checksum = Arc::new(AtomicU64::new(0));
    let warmup_vals = Arc::new(make_values(cfg.ops.min(10_000), 0x33));
    let bench_vals = Arc::new(make_values(cfg.ops, 0x44));
    let (addr, handle) = sync::spawn_with_opts(
        AfSyncTryTellActor {
            processed: Arc::clone(&processed),
            checksum: Arc::clone(&checksum),
        },
        sync::SpawnOpts {
            mailbox_capacity: cfg.mailbox_cap,
            ..sync::SpawnOpts::default()
        },
    );
    handle.wait_for_startup();

    let warmup = warmup_vals.len() as u64;
    for &v in warmup_vals.iter() {
        let mut spins = 0u32;
        while !addr.tell(v) {
            spin_then_yield(&mut spins);
        }
    }
    while processed.load(Ordering::Relaxed) < warmup {
        let mut spins = 0u32;
        spin_then_yield(&mut spins);
    }

    let t0 = Instant::now();
    let per_producer = split_ops(cfg.ops, cfg.producers);
    for _ in 0..iters {
        processed.store(0, Ordering::Relaxed);
        checksum.store(0, Ordering::Relaxed);
        let mut joins = Vec::with_capacity(cfg.producers);
        for ops in per_producer.iter().copied() {
            let addr = addr.clone();
            let bench_vals = Arc::clone(&bench_vals);
            joins.push(thread::spawn(move || {
                for idx in 0..ops as usize {
                    let mut spins = 0u32;
                    let v = bench_vals[idx];
                    while !addr.tell(v) {
                        spin_then_yield(&mut spins);
                    }
                }
            }));
        }
        for join in joins {
            join.join().expect("actor-framework sync actor-ref producer panicked");
        }
        while processed.load(Ordering::Relaxed) < cfg.ops {
            let mut spins = 0u32;
            spin_then_yield(&mut spins);
        }
        std::hint::black_box(checksum.load(Ordering::Relaxed));
    }
    let elapsed = t0.elapsed();
    handle.shutdown();
    elapsed
}

fn run_actix_sync_tell(cfg: Config, iters: u64) -> Duration {
    let ops = cfg.ops;
    let threads = cfg.actix_sync_threads;
    let producers = cfg.producers;
    let warmup_vals = Arc::new(make_values(cfg.ops.min(10_000), 0x55));
    let bench_vals = Arc::new(make_values(cfg.ops, 0x66));
    let sys = System::new();

    sys.block_on(async move {
        let processed = Arc::new(AtomicU64::new(0));
        let checksum = Arc::new(AtomicU64::new(0));
        let processed_for_factory = Arc::clone(&processed);
        let checksum_for_factory = Arc::clone(&checksum);
        let addr = SyncArbiter::start(threads, move || ActixSyncTryTellActor {
            processed: Arc::clone(&processed_for_factory),
            checksum: Arc::clone(&checksum_for_factory),
        });
        let per_producer = split_ops(ops, producers);

        let warmup = warmup_vals.len() as u64;
        for &v in warmup_vals.iter() {
            addr.do_send(ActixTryTellMsg(v));
        }
        while processed.load(Ordering::Relaxed) < warmup {
            tokio::task::yield_now().await;
        }

        let t0 = Instant::now();
        for _ in 0..iters {
            processed.store(0, Ordering::Relaxed);
            checksum.store(0, Ordering::Relaxed);
            let mut joins = Vec::with_capacity(producers);
            for ops in per_producer.iter().copied() {
                let addr = addr.clone();
                let bench_vals = Arc::clone(&bench_vals);
                joins.push(thread::spawn(move || {
                    for idx in 0..ops as usize {
                        addr.do_send(ActixTryTellMsg(bench_vals[idx]));
                    }
                }));
            }
            for join in joins {
                join.join().expect("actix sync producer panicked");
            }
            while processed.load(Ordering::Relaxed) < ops {
                tokio::task::yield_now().await;
            }
            std::hint::black_box(checksum.load(Ordering::Relaxed));
        }
        t0.elapsed()
    })
}

fn benchmark(c: &mut Criterion) {
    let cfg = Config::from_env();
    let mut group = c.benchmark_group("compare_sync_tell_current");
    group.sample_size(cfg.sample_size);
    group.warm_up_time(Duration::from_millis(cfg.warmup_ms.max(100)));
    group.measurement_time(Duration::from_millis(cfg.measurement_ms.max(500)));
    group.throughput(Throughput::Elements(cfg.ops));

    group.bench_function("actor_framework_sync_snapshot", move |b| {
        b.iter_custom(|iters| run_af_sync_tell_snapshot(cfg, iters));
    });

    group.bench_function("actor_framework_sync", move |b| {
        b.iter_custom(|iters| run_af_sync_tell_actor_ref(cfg, iters));
    });

    group.bench_function("actix_sync", move |b| {
        b.iter_custom(|iters| run_actix_sync_tell(cfg, iters));
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().configure_from_args();
    targets = benchmark
}
criterion_main!(benches);
