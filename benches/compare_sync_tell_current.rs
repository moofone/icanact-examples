use actix::{
    Actor as ActixActor, ActorContext as _, Handler as ActixHandler, Message as ActixMessage,
    SyncArbiter, SyncContext, System,
};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use icanact_core::{Tell, local_sync as sync, local_sync::SyncActor};
use std::hint::black_box;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug)]
struct Config {
    ops: u64,
    mailbox_cap: usize,
    actix_sync_threads: usize,
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
            sample_size: parse_usize_env("AF_CMP_SAMPLE_SIZE", 10).clamp(10, 100),
            warmup_ms: parse_u64_env("AF_CMP_WARMUP_MS", 150),
            measurement_ms: parse_u64_env("AF_CMP_MEASUREMENT_MS", 700),
        }
    }
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
}

impl SyncActor for AfSyncTryTellActor {}

impl Tell for AfSyncTryTellActor {
    type Msg = u64;

    #[inline(always)]
    fn handle_tell(&mut self, _msg: icanact_core::ActorMessage<Self::Msg>) {
        self.processed.fetch_add(1, Ordering::Relaxed);
    }
}

struct ActixSyncTryTellActor {
    processed: Arc<AtomicU64>,
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
    fn handle(&mut self, _msg: ActixTryTellMsg, _ctx: &mut Self::Context) -> Self::Result {
        self.processed.fetch_add(1, Ordering::Relaxed);
    }
}

fn run_af_sync_tell(cfg: Config, iters: u64) -> Duration {
    let processed = Arc::new(AtomicU64::new(0));
    let (actor_ref, handle) = sync::spawn_with_opts(
        AfSyncTryTellActor {
            processed: Arc::clone(&processed),
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

    let warmup = cfg.ops.min(10_000);
    for _ in 0..warmup {
        let mut spins = 0u32;
        while !addr.tell(sync::SyncTellMessage::Tell(1)) {
            spin_then_yield(&mut spins);
        }
    }
    while processed.load(Ordering::Relaxed) < warmup {
        let mut spins = 0u32;
        spin_then_yield(&mut spins);
    }

    let t0 = Instant::now();
    for _ in 0..iters {
        processed.store(0, Ordering::Relaxed);
        for _ in 0..cfg.ops {
            let mut spins = 0u32;
            while !addr.tell(sync::SyncTellMessage::Tell(1)) {
                spin_then_yield(&mut spins);
            }
        }
        while processed.load(Ordering::Relaxed) < cfg.ops {
            let mut spins = 0u32;
            spin_then_yield(&mut spins);
        }
    }
    let elapsed = t0.elapsed();
    handle.shutdown();
    elapsed
}

fn run_af_sync_tell_actor_ref(cfg: Config, iters: u64) -> Duration {
    let processed = Arc::new(AtomicU64::new(0));
    let (addr, handle) = sync::spawn_with_opts(
        AfSyncTryTellActor {
            processed: Arc::clone(&processed),
        },
        sync::SpawnOpts {
            mailbox_capacity: cfg.mailbox_cap,
            ..sync::SpawnOpts::default()
        },
    );
    handle.wait_for_startup();

    let warmup = cfg.ops.min(10_000);
    for _ in 0..warmup {
        let mut spins = 0u32;
        while !addr.tell(1) {
            spin_then_yield(&mut spins);
        }
    }
    while processed.load(Ordering::Relaxed) < warmup {
        let mut spins = 0u32;
        spin_then_yield(&mut spins);
    }

    let t0 = Instant::now();
    for _ in 0..iters {
        processed.store(0, Ordering::Relaxed);
        for _ in 0..cfg.ops {
            let mut spins = 0u32;
            while !addr.tell(1) {
                spin_then_yield(&mut spins);
            }
        }
        while processed.load(Ordering::Relaxed) < cfg.ops {
            let mut spins = 0u32;
            spin_then_yield(&mut spins);
        }
    }
    let elapsed = t0.elapsed();
    handle.shutdown();
    elapsed
}

fn run_actix_sync_tell(cfg: Config, iters: u64) -> Duration {
    let ops = cfg.ops;
    let threads = cfg.actix_sync_threads;
    let sys = System::new();

    sys.block_on(async move {
        let processed = Arc::new(AtomicU64::new(0));
        let processed_for_factory = Arc::clone(&processed);
        let addr = SyncArbiter::start(threads, move || ActixSyncTryTellActor {
            processed: Arc::clone(&processed_for_factory),
        });

        let warmup = ops.min(10_000);
        for _ in 0..warmup {
            addr.do_send(ActixTryTellMsg(1));
        }
        while processed.load(Ordering::Relaxed) < warmup {
            tokio::task::yield_now().await;
        }

        let t0 = Instant::now();
        for _ in 0..iters {
            processed.store(0, Ordering::Relaxed);
            for _ in 0..ops {
                addr.do_send(ActixTryTellMsg(1));
            }
            while processed.load(Ordering::Relaxed) < ops {
                tokio::task::yield_now().await;
            }
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

    group.bench_function("actor_framework_sync", move |b| {
        b.iter_custom(|iters| run_af_sync_tell(cfg, iters));
    });

    group.bench_function("actor_framework_sync_actor_ref", move |b| {
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
