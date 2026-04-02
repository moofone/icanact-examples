use actix::{
    Actor as ActixActor, Context as ActixContext, Handler as ActixHandler,
    Message as ActixMessage, System,
};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use icanact_core::local_async::{self, AsyncActor};
use kameo::actor::Spawn as KameoSpawn;
use kameo::mailbox;
use kameo::message::{Context as KameoContext, Message as KameoMessage};
use rand::rngs::SmallRng;
use rand::{RngCore, SeedableRng};
use ractor::{
    Actor as RactorActor, ActorProcessingErr as RactorActorProcessingErr,
    ActorRef as RactorActorRef,
};
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
            mailbox_cap: parse_usize_env("AF_CMP_ASYNC_MAILBOX_CAP", 65_536).max(1),
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

struct AfAsyncTellActor {
    processed: Arc<AtomicU64>,
    checksum: Arc<AtomicU64>,
}

impl AsyncActor for AfAsyncTellActor {
    type Contract = local_async::contract::TellOnly;
    type Tell = u64;
    type Ask = ();
    type Reply = ();

    #[inline(always)]
    async fn handle_tell(&mut self, msg: Self::Tell) {
        self.processed.fetch_add(1, Ordering::Relaxed);
        self.checksum.fetch_add(msg, Ordering::Relaxed);
    }
}

struct ActixAsyncTellActor {
    processed: Arc<AtomicU64>,
    checksum: Arc<AtomicU64>,
    mailbox_cap: usize,
}

#[derive(ActixMessage)]
#[rtype(result = "()")]
struct ActixTellMsg(u64);

impl ActixActor for ActixAsyncTellActor {
    type Context = ActixContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.set_mailbox_capacity(self.mailbox_cap);
    }
}

impl ActixHandler<ActixTellMsg> for ActixAsyncTellActor {
    type Result = ();

    #[inline(always)]
    fn handle(&mut self, msg: ActixTellMsg, _ctx: &mut Self::Context) -> Self::Result {
        self.processed.fetch_add(1, Ordering::Relaxed);
        self.checksum.fetch_add(msg.0, Ordering::Relaxed);
    }
}

struct RactorAsyncTellActor;

impl RactorActor for RactorAsyncTellActor {
    type Msg = u64;
    type State = (Arc<AtomicU64>, Arc<AtomicU64>);
    type Arguments = (Arc<AtomicU64>, Arc<AtomicU64>);

    #[inline(always)]
    fn pre_start(
        &self,
        _myself: RactorActorRef<Self::Msg>,
        state: Self::Arguments,
    ) -> impl std::future::Future<Output = Result<Self::State, RactorActorProcessingErr>> + Send
    {
        std::future::ready(Ok(state))
    }

    #[inline(always)]
    fn handle(
        &self,
        _myself: RactorActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> impl std::future::Future<Output = Result<(), RactorActorProcessingErr>> + Send {
        state.0.fetch_add(1, Ordering::Relaxed);
        state.1.fetch_add(message, Ordering::Relaxed);
        std::future::ready(Ok(()))
    }
}

#[derive(kameo::Actor)]
struct KameoTellActor {
    processed: Arc<AtomicU64>,
    checksum: Arc<AtomicU64>,
}

struct KameoTellMsg(u64);

impl KameoMessage<KameoTellMsg> for KameoTellActor {
    type Reply = ();

    #[inline(always)]
    async fn handle(
        &mut self,
        msg: KameoTellMsg,
        _ctx: &mut KameoContext<Self, Self::Reply>,
    ) -> Self::Reply {
        self.processed.fetch_add(1, Ordering::Relaxed);
        self.checksum.fetch_add(msg.0, Ordering::Relaxed);
    }
}

fn async_runtime_builder(producers: usize) -> tokio::runtime::Builder {
    if producers <= 1 {
        tokio::runtime::Builder::new_current_thread()
    } else {
        let mut b = tokio::runtime::Builder::new_multi_thread();
        b.worker_threads(producers);
        b
    }
}

fn run_af_async_tell(cfg: Config, iters: u64) -> Duration {
    let ops = cfg.ops;
    let cap = cfg.mailbox_cap;
    let producers = cfg.producers;
    let warmup_vals = Arc::new(make_values(cfg.ops.min(10_000), 0x101));
    let bench_vals = Arc::new(make_values(cfg.ops, 0x202));
    let rt = async_runtime_builder(producers)
        .enable_time()
        .build()
        .expect("failed to build tokio runtime");

    rt.block_on(async move {
        let processed = Arc::new(AtomicU64::new(0));
        let checksum = Arc::new(AtomicU64::new(0));
        let (addr, handle) = local_async::spawn_with_opts(
            AfAsyncTellActor {
                processed: Arc::clone(&processed),
                checksum: Arc::clone(&checksum),
            },
            local_async::SpawnOpts {
                mailbox_capacity: cap,
                ..Default::default()
            },
        )
        .await;
        let per_producer = split_ops(ops, producers);

        let warmup = warmup_vals.len() as u64;
        for &v in warmup_vals.iter() {
            while !addr.tell(v) {
                tokio::task::yield_now().await;
            }
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
                joins.push(tokio::spawn(async move {
                    for idx in 0..ops as usize {
                        while !addr.tell(bench_vals[idx]) {
                            tokio::task::yield_now().await;
                        }
                    }
                }));
            }
            for join in joins {
                join.await.expect("actor-framework async producer panicked");
            }
            while processed.load(Ordering::Relaxed) < ops {
                tokio::task::yield_now().await;
            }
            black_box(checksum.load(Ordering::Relaxed));
        }
        let elapsed = t0.elapsed();
        handle.shutdown().await;
        elapsed
    })
}

fn run_actix_async_tell(cfg: Config, iters: u64) -> Duration {
    let ops = cfg.ops;
    let producers = cfg.producers;
    let warmup_vals = Arc::new(make_values(cfg.ops.min(10_000), 0x303));
    let bench_vals = Arc::new(make_values(cfg.ops, 0x404));
    let sys = System::new();

    sys.block_on(async move {
        let processed = Arc::new(AtomicU64::new(0));
        let checksum = Arc::new(AtomicU64::new(0));
        let addr = ActixAsyncTellActor {
            processed: Arc::clone(&processed),
            checksum: Arc::clone(&checksum),
            mailbox_cap: cfg.mailbox_cap,
        }
        .start();
        let per_producer = split_ops(ops, producers);

        let warmup = warmup_vals.len() as u64;
        for &v in warmup_vals.iter() {
            addr.do_send(ActixTellMsg(v));
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
                joins.push(std::thread::spawn(move || {
                    for idx in 0..ops as usize {
                        addr.do_send(ActixTellMsg(bench_vals[idx]));
                    }
                }));
            }
            for join in joins {
                join.join().expect("actix async producer panicked");
            }
            while processed.load(Ordering::Relaxed) < ops {
                tokio::task::yield_now().await;
            }
            black_box(checksum.load(Ordering::Relaxed));
        }
        t0.elapsed()
    })
}

fn run_ractor_async_tell(cfg: Config, iters: u64) -> Duration {
    let ops = cfg.ops;
    let producers = cfg.producers;
    let warmup_vals = Arc::new(make_values(cfg.ops.min(10_000), 0x505));
    let bench_vals = Arc::new(make_values(cfg.ops, 0x606));
    let rt = async_runtime_builder(producers)
        .enable_time()
        .build()
        .expect("failed to build tokio runtime");

    rt.block_on(async move {
        let processed = Arc::new(AtomicU64::new(0));
        let checksum = Arc::new(AtomicU64::new(0));
        let (addr, handle) =
            RactorAsyncTellActor::spawn(
                None,
                RactorAsyncTellActor,
                (Arc::clone(&processed), Arc::clone(&checksum)),
            )
                .await
                .expect("failed to spawn ractor async actor");
        let per_producer = split_ops(ops, producers);

        let warmup = warmup_vals.len() as u64;
        for &v in warmup_vals.iter() {
            addr.send_message(v)
                .expect("ractor async tell warmup send failed");
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
                joins.push(tokio::task::spawn_blocking(move || {
                    for idx in 0..ops as usize {
                        addr.send_message(bench_vals[idx])
                            .expect("ractor async tell send failed");
                    }
                }));
            }
            for join in joins {
                join.await.expect("ractor async producer panicked");
            }
            while processed.load(Ordering::Relaxed) < ops {
                tokio::task::yield_now().await;
            }
            black_box(checksum.load(Ordering::Relaxed));
        }
        let elapsed = t0.elapsed();
        addr.stop(None);
        handle.await.expect("ractor async actor join failed");
        elapsed
    })
}

fn run_kameo_async_tell(cfg: Config, iters: u64) -> Duration {
    let ops = cfg.ops;
    let cap = cfg.mailbox_cap;
    let producers = cfg.producers;
    let warmup_vals = Arc::new(make_values(cfg.ops.min(10_000), 0x707));
    let bench_vals = Arc::new(make_values(cfg.ops, 0x808));
    let rt = async_runtime_builder(producers)
        .enable_time()
        .build()
        .expect("kameo tell runtime");

    rt.block_on(async move {
        let processed = Arc::new(AtomicU64::new(0));
        let checksum = Arc::new(AtomicU64::new(0));
        let actor_ref = KameoTellActor::spawn_with_mailbox(
            KameoTellActor {
                processed: Arc::clone(&processed),
                checksum: Arc::clone(&checksum),
            },
            mailbox::bounded(cap),
        );
        let per_producer = split_ops(ops, producers);

        let warmup = warmup_vals.len() as u64;
        for &v in warmup_vals.iter() {
            actor_ref
                .tell(KameoTellMsg(v))
                .send()
                .await
                .expect("kameo tell warmup send failed");
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
                let actor_ref = actor_ref.clone();
                let bench_vals = Arc::clone(&bench_vals);
                joins.push(tokio::spawn(async move {
                    for idx in 0..ops as usize {
                        actor_ref
                            .tell(KameoTellMsg(bench_vals[idx]))
                            .send()
                            .await
                            .expect("kameo tell send failed");
                    }
                }));
            }
            for join in joins {
                join.await.expect("kameo async producer panicked");
            }
            while processed.load(Ordering::Relaxed) < ops {
                tokio::task::yield_now().await;
            }
            black_box(checksum.load(Ordering::Relaxed));
        }
        t0.elapsed()
    })
}

fn benchmark(c: &mut Criterion) {
    let cfg = Config::from_env();
    let mut group = c.benchmark_group("compare_async_tell_current");
    group.sample_size(cfg.sample_size);
    group.warm_up_time(Duration::from_millis(cfg.warmup_ms.max(100)));
    group.measurement_time(Duration::from_millis(cfg.measurement_ms.max(500)));
    group.throughput(Throughput::Elements(cfg.ops));

    group.bench_function("actor_framework_async", move |b| {
        b.iter_custom(|iters| run_af_async_tell(cfg, iters));
    });
    group.bench_function("actix_async", move |b| {
        b.iter_custom(|iters| run_actix_async_tell(cfg, iters));
    });
    group.bench_function("ractor_async", move |b| {
        b.iter_custom(|iters| run_ractor_async_tell(cfg, iters));
    });
    group.bench_function("kameo_async", move |b| {
        b.iter_custom(|iters| run_kameo_async_tell(cfg, iters));
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().configure_from_args();
    targets = benchmark
}
criterion_main!(benches);
