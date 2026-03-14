use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

use actix::prelude::{
    Actor as ActixActor, Handler, SyncArbiter, SyncContext, System,
};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use icanact_core::local_sync as af_sync;
use kameo::actor::Spawn as KameoSpawn;
use kameo::mailbox;
use kameo::message::{Context as KameoContext, Message as KameoMessage};

#[derive(Clone, Copy)]
struct Config {
    tell_ops: u64,
    ask_ops: u64,
    sample_size: usize,
}

impl Config {
    fn from_env() -> Self {
        Self {
            tell_ops: parse_u64_env("AF_KAMEO_TELL_OPS", 1_000_000),
            ask_ops: parse_u64_env("AF_KAMEO_ASK_OPS", 250_000),
            sample_size: parse_usize_env("AF_KAMEO_SAMPLE_SIZE", 10).clamp(10, 100),
        }
    }
}

fn parse_u64_env(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(default)
        .max(1)
}

fn parse_usize_env(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(default)
        .max(1)
}

struct ActixTellActor {
    processed: Arc<AtomicU64>,
}

impl ActixActor for ActixTellActor {
    type Context = SyncContext<Self>;
}

#[derive(actix::Message)]
#[rtype(result = "()")]
struct ActixTellMsg(u64);

impl Handler<ActixTellMsg> for ActixTellActor {
    type Result = ();

    fn handle(&mut self, msg: ActixTellMsg, _ctx: &mut Self::Context) -> Self::Result {
        self.processed.fetch_add(msg.0, Ordering::Relaxed);
    }
}

struct ActixAskActor;

impl ActixActor for ActixAskActor {
    type Context = SyncContext<Self>;
}

#[derive(actix::Message)]
#[rtype(result = "u64")]
struct ActixAskMsg(u64);

impl Handler<ActixAskMsg> for ActixAskActor {
    type Result = u64;

    fn handle(&mut self, msg: ActixAskMsg, _ctx: &mut Self::Context) -> Self::Result {
        msg.0
    }
}

#[derive(kameo::Actor)]
struct KameoTellActor {
    processed: Arc<AtomicU64>,
}

struct KameoTellMsg(u64);

impl KameoMessage<KameoTellMsg> for KameoTellActor {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: KameoTellMsg,
        _ctx: &mut KameoContext<Self, Self::Reply>,
    ) -> Self::Reply {
        self.processed.fetch_add(msg.0, Ordering::Relaxed);
    }
}

#[derive(kameo::Actor)]
struct KameoAskActor;

struct KameoAskMsg(u64);

impl KameoMessage<KameoAskMsg> for KameoAskActor {
    type Reply = u64;

    async fn handle(
        &mut self,
        msg: KameoAskMsg,
        _ctx: &mut KameoContext<Self, Self::Reply>,
    ) -> Self::Reply {
        msg.0
    }
}

enum AfAskMsg {
    Echo {
        v: u64,
        reply: af_sync::ReplyTo<u64>,
    },
}

fn run_af_tell(cfg: Config, iters: u64) -> Duration {
    let processed = Arc::new(AtomicU64::new(0));
    let processed_for_actor = Arc::clone(&processed);
    let (addr, handle) = af_sync::mpsc::spawn(65_536, move |msg: u64| {
        processed_for_actor.fetch_add(msg, Ordering::Relaxed);
    });

    let start = Instant::now();
        for _ in 0..iters {
            processed.store(0, Ordering::Relaxed);
            for _ in 0..cfg.tell_ops {
                while !addr.tell(1) {
                    std::hint::spin_loop();
                }
            }
            while processed.load(Ordering::Relaxed) < cfg.tell_ops {
                std::thread::yield_now();
        }
    }
    let elapsed = start.elapsed();
    handle.shutdown();
    elapsed
}

fn run_actix_tell(cfg: Config, iters: u64) -> Duration {
    let sys = System::new();
    sys.block_on(async move {
        let processed = Arc::new(AtomicU64::new(0));
        let processed_for_factory = Arc::clone(&processed);
        let addr = SyncArbiter::start(1, move || ActixTellActor {
            processed: Arc::clone(&processed_for_factory),
        });

        let start = Instant::now();
        for _ in 0..iters {
            processed.store(0, Ordering::Relaxed);
            for _ in 0..cfg.tell_ops {
                addr.do_send(ActixTellMsg(1));
            }
            while processed.load(Ordering::Relaxed) < cfg.tell_ops {
                tokio::task::yield_now().await;
            }
        }
        start.elapsed()
    })
}

fn run_kameo_tell(cfg: Config, iters: u64) -> Duration {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("kameo tell runtime");

    rt.block_on(async move {
        let processed = Arc::new(AtomicU64::new(0));
        let actor_ref = KameoTellActor::spawn_with_mailbox(
            KameoTellActor {
                processed: Arc::clone(&processed),
            },
            mailbox::bounded(65_536),
        );

        let start = Instant::now();
        for _ in 0..iters {
            processed.store(0, Ordering::Relaxed);
            for _ in 0..cfg.tell_ops {
                loop {
                    match actor_ref.tell(KameoTellMsg(1)).try_send() {
                        Ok(()) => break,
                        Err(_) => tokio::task::yield_now().await,
                    }
                }
            }
            while processed.load(Ordering::Relaxed) < cfg.tell_ops {
                tokio::task::yield_now().await;
            }
        }
        start.elapsed()
    })
}

fn run_af_ask(cfg: Config, iters: u64) -> Duration {
    let (addr, handle) = af_sync::mpsc::spawn(65_536, move |msg: AfAskMsg| match msg {
        AfAskMsg::Echo { v, reply } => {
            let _ = reply.reply(v);
        }
    });

    let start = Instant::now();
        for _ in 0..iters {
            for i in 0..cfg.ask_ops {
                let out = addr
                    .ask_timeout(
                        |reply| AfAskMsg::Echo { v: i, reply },
                        Duration::from_secs(5),
                    )
                    .expect("af ask failed");
                assert_eq!(out, i);
            }
        }
    let elapsed = start.elapsed();
    handle.shutdown();
    elapsed
}

fn run_actix_ask(cfg: Config, iters: u64) -> Duration {
    let sys = System::new();
    sys.block_on(async move {
        let addr = SyncArbiter::start(1, || ActixAskActor);
        let start = Instant::now();
        for _ in 0..iters {
            for i in 0..cfg.ask_ops {
                let out = addr.send(ActixAskMsg(i)).await.expect("actix ask failed");
                assert_eq!(out, i);
            }
        }
        start.elapsed()
    })
}

fn run_kameo_ask(cfg: Config, iters: u64) -> Duration {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("kameo ask runtime");

    rt.block_on(async move {
        let actor_ref = KameoAskActor::spawn(KameoAskActor);
        let start = Instant::now();
        for _ in 0..iters {
            for i in 0..cfg.ask_ops {
                let out = actor_ref
                    .ask(KameoAskMsg(i))
                    .send()
                    .await
                    .expect("kameo ask failed");
                assert_eq!(out, i);
            }
        }
        start.elapsed()
    })
}

fn bench_kameo_compare(c: &mut Criterion) {
    let cfg = Config::from_env();

    let mut tell_group = c.benchmark_group("tell_compare_af_actix_kameo");
    tell_group.sample_size(cfg.sample_size);
    tell_group.throughput(Throughput::Elements(cfg.tell_ops));
    tell_group.bench_function(BenchmarkId::new("actor_framework_sync", cfg.tell_ops), |b| {
        b.iter_custom(|iters| run_af_tell(cfg, iters));
    });
    tell_group.bench_function(BenchmarkId::new("actix_sync", cfg.tell_ops), |b| {
        b.iter_custom(|iters| run_actix_tell(cfg, iters));
    });
    tell_group.bench_function(BenchmarkId::new("kameo_async", cfg.tell_ops), |b| {
        b.iter_custom(|iters| run_kameo_tell(cfg, iters));
    });
    tell_group.finish();

    let mut ask_group = c.benchmark_group("ask_compare_af_actix_kameo");
    ask_group.sample_size(cfg.sample_size);
    ask_group.throughput(Throughput::Elements(cfg.ask_ops));
    ask_group.bench_function(BenchmarkId::new("actor_framework_sync", cfg.ask_ops), |b| {
        b.iter_custom(|iters| run_af_ask(cfg, iters));
    });
    ask_group.bench_function(BenchmarkId::new("actix_sync", cfg.ask_ops), |b| {
        b.iter_custom(|iters| run_actix_ask(cfg, iters));
    });
    ask_group.bench_function(BenchmarkId::new("kameo_async", cfg.ask_ops), |b| {
        b.iter_custom(|iters| run_kameo_ask(cfg, iters));
    });
    ask_group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_millis(1500));
    targets = bench_kameo_compare
}
criterion_main!(benches);
