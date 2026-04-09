use actix::{
    Actor as ActixActor, Context as ActixContext, Handler as ActixHandler, Message as ActixMessage,
    SyncArbiter, SyncContext, System,
};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use icanact_core::local_async::{self, AsyncActor};
use icanact_core::local_sync;
use std::hint::black_box;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug)]
struct Config {
    ops: u64,
    async_mailbox_cap: usize,
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
        let ops = parse_u64_env("AF_CMP_SYNC_ASYNC_ASK_OPS", 100_000).max(1);
        Self {
            ops,
            async_mailbox_cap: parse_usize_env(
                "AF_CMP_ASYNC_MAILBOX_CAP",
                usize::try_from(ops).unwrap_or(1_000_000).max(1_000_000),
            )
            .max(1),
            sample_size: parse_usize_env("AF_CMP_SAMPLE_SIZE", 10).clamp(10, 100),
            warmup_ms: parse_u64_env("AF_CMP_WARMUP_MS", 150),
            measurement_ms: parse_u64_env("AF_CMP_MEASUREMENT_MS", 700),
        }
    }
}

#[derive(Clone, Copy)]
enum AfSyncSinkMsg {
    Done(u64),
}

enum AfAsyncReq {
    Fetch {
        v: u64,
        reply: local_sync::TellResult<AfSyncSinkMsg>,
    },
}

struct AfAsyncWorker;

impl AsyncActor for AfAsyncWorker {
    type Contract = local_async::contract::TellOnly;
    type Tell = AfAsyncReq;
    type Ask = ();
    type Reply = ();
    type Channel = ();
    type PubSub = ();
    type Broadcast = ();

    #[inline(always)]
    async fn handle_tell(&mut self, msg: Self::Tell) {
        match msg {
            AfAsyncReq::Fetch { v, reply } => {
                let _ = reply.reply(AfSyncSinkMsg::Done(v.wrapping_add(1)));
            }
        }
    }
}

struct ActixSyncSink {
    processed: Arc<AtomicU64>,
    checksum: Arc<AtomicU64>,
}

#[derive(ActixMessage)]
#[rtype(result = "()")]
struct ActixSyncDone(u64);

impl ActixActor for ActixSyncSink {
    type Context = SyncContext<Self>;
}

impl ActixHandler<ActixSyncDone> for ActixSyncSink {
    type Result = ();

    #[inline(always)]
    fn handle(&mut self, msg: ActixSyncDone, _ctx: &mut Self::Context) -> Self::Result {
        self.processed.fetch_add(1, Ordering::Relaxed);
        self.checksum.fetch_add(msg.0, Ordering::Relaxed);
    }
}

#[derive(ActixMessage)]
#[rtype(result = "()")]
struct ActixAsyncReq {
    v: u64,
    reply_to: actix::Addr<ActixSyncSink>,
}

struct ActixAsyncWorker {
    mailbox_cap: usize,
}

impl ActixActor for ActixAsyncWorker {
    type Context = ActixContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.set_mailbox_capacity(self.mailbox_cap);
    }
}

impl ActixHandler<ActixAsyncReq> for ActixAsyncWorker {
    type Result = ();

    #[inline(always)]
    fn handle(&mut self, msg: ActixAsyncReq, _ctx: &mut Self::Context) -> Self::Result {
        let _ = msg.reply_to.try_send(ActixSyncDone(msg.v.wrapping_add(1)));
    }
}

fn run_af_sync_async_ask(cfg: Config, iters: u64) -> Duration {
    let ops = cfg.ops;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");

    rt.block_on(async move {
        let processed = Arc::new(AtomicU64::new(0));
        let checksum = Arc::new(AtomicU64::new(0));
        let (sink_addr, sink_handle) = local_sync::mpsc::spawn(cfg.async_mailbox_cap, {
            let processed = Arc::clone(&processed);
            let checksum = Arc::clone(&checksum);
            move |msg: AfSyncSinkMsg| match msg {
                AfSyncSinkMsg::Done(v) => {
                    processed.fetch_add(1, Ordering::Relaxed);
                    checksum.fetch_add(v, Ordering::Relaxed);
                }
            }
        });
        let (worker_ref, worker_handle) = local_async::spawn_with_opts(
            AfAsyncWorker,
            local_async::SpawnOpts {
                mailbox_capacity: cfg.async_mailbox_cap,
                ..Default::default()
            },
        )
        .await;

        let warmup = ops.min(10_000);
        for i in 0..warmup {
            while !worker_ref.tell_result_to(&sink_addr, |reply| AfAsyncReq::Fetch { v: i, reply })
            {
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
            for i in 0..ops {
                while !worker_ref.tell_result_to(&sink_addr, |reply| AfAsyncReq::Fetch {
                    v: black_box(i),
                    reply,
                }) {
                    tokio::task::yield_now().await;
                }
            }
            while processed.load(Ordering::Relaxed) < ops {
                tokio::task::yield_now().await;
            }
            black_box(checksum.load(Ordering::Relaxed));
        }
        let elapsed = t0.elapsed();

        worker_handle.shutdown().await;
        sink_handle.shutdown();
        elapsed
    })
}

fn run_actix_sync_async_ask(cfg: Config, iters: u64) -> Duration {
    let ops = cfg.ops;
    let sys = System::new();

    sys.block_on(async move {
        let processed = Arc::new(AtomicU64::new(0));
        let checksum = Arc::new(AtomicU64::new(0));
        let processed_for_factory = Arc::clone(&processed);
        let checksum_for_factory = Arc::clone(&checksum);
        let sink_addr = SyncArbiter::start(1, move || ActixSyncSink {
            processed: Arc::clone(&processed_for_factory),
            checksum: Arc::clone(&checksum_for_factory),
        });
        let worker_addr = ActixAsyncWorker {
            mailbox_cap: cfg.async_mailbox_cap,
        }
        .start();

        // Apples-to-apples note: we intentionally do not use `Addr::send(...).await` here,
        // because that makes the caller own the wait. The framework comparison route is a
        // tell-style request carrying an explicit reply target back into a sync actor.
        let warmup = ops.min(10_000);
        for i in 0..warmup {
            loop {
                match worker_addr.try_send(ActixAsyncReq {
                    v: i,
                    reply_to: sink_addr.clone(),
                }) {
                    Ok(()) => break,
                    Err(actix::prelude::SendError::Full(_)) => tokio::task::yield_now().await,
                    Err(actix::prelude::SendError::Closed(_)) => {
                        panic!("actix async worker closed")
                    }
                }
            }
        }
        while processed.load(Ordering::Relaxed) < warmup {
            tokio::task::yield_now().await;
        }

        let t0 = Instant::now();
        for _ in 0..iters {
            processed.store(0, Ordering::Relaxed);
            checksum.store(0, Ordering::Relaxed);
            for i in 0..ops {
                loop {
                    match worker_addr.try_send(ActixAsyncReq {
                        v: black_box(i),
                        reply_to: sink_addr.clone(),
                    }) {
                        Ok(()) => break,
                        Err(actix::prelude::SendError::Full(_)) => tokio::task::yield_now().await,
                        Err(actix::prelude::SendError::Closed(_)) => {
                            panic!("actix async worker closed")
                        }
                    }
                }
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
    let mut group = c.benchmark_group("compare_sync_async_ask_current");
    group.sample_size(cfg.sample_size);
    group.warm_up_time(Duration::from_millis(cfg.warmup_ms.max(100)));
    group.measurement_time(Duration::from_millis(cfg.measurement_ms.max(500)));
    group.throughput(Throughput::Elements(cfg.ops));

    group.bench_function("actor_framework_sync_to_async_tell_result_to", move |b| {
        b.iter_custom(|iters| run_af_sync_async_ask(cfg, iters));
    });

    group.bench_function("actix_sync_to_async_reply_to_sync", move |b| {
        b.iter_custom(|iters| run_actix_sync_async_ask(cfg, iters));
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().configure_from_args();
    targets = benchmark
}
criterion_main!(benches);
