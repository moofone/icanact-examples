use actix::prelude::{
    Actor as ActixActor, Context as ActixContext, Handler as ActixHandler,
    Message as ActixMessage, SyncArbiter, SyncContext, System,
};
use icanact_core::local_async;
use icanact_core::local_async::tell::Tell as AfAsyncTell;
use icanact_core::local_sync as sync;
use icanact_core::{ActorMessage, Tell};
use kameo::actor::Spawn as KameoSpawn;
use kameo::mailbox;
use kameo::message::{Context as KameoContext, Message as KameoMessage};
use ractor::{
    Actor as RactorActor, ActorProcessingErr as RactorActorProcessingErr,
    ActorRef as RactorActorRef,
};
use std::env;
use std::hint::black_box;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::Instant;

#[derive(Clone, Copy, Debug)]
enum Mode {
    Sync,
    Async,
    All,
}

impl Mode {
    fn parse(raw: &str) -> Result<Self, String> {
        match raw {
            "sync" => Ok(Self::Sync),
            "async" => Ok(Self::Async),
            "all" => Ok(Self::All),
            other => Err(format!("invalid --mode '{other}', expected sync|async|all")),
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct Config {
    mode: Mode,
    sync_ops: u64,
    async_ops: u64,
    cap: usize,
    producers: usize,
}

fn usage() -> &'static str {
    "Usage: cargo bench --bench framework_tell_snapshot -- [options]\n\
     \n\
     Environment overrides:\n\
       AF_SNAPSHOT_TELL_MODE       sync|async|all (default: all)\n\
       AF_SNAPSHOT_SYNC_TELL_OPS   sync tell ops per run (default: 300000)\n\
       AF_SNAPSHOT_ASYNC_TELL_OPS  async tell ops per run (default: 300000)\n\
       AF_SNAPSHOT_MAILBOX_CAP     mailbox capacity (default: 300000)\n\
       AF_SNAPSHOT_TELL_PRODUCERS  publishers/tasks (default: 1)\n\
     \n\
     CLI options:\n\
       --mode sync|async|all\n\
       --sync-ops N\n\
       --async-ops N\n\
       --cap N\n\
       --producers N\n"
}

fn parse_u64(raw: &str, flag: &str) -> Result<u64, String> {
    raw.parse::<u64>()
        .map_err(|err| format!("invalid {flag} value '{raw}': {err}"))
}

fn parse_usize(raw: &str, flag: &str) -> Result<usize, String> {
    raw.parse::<usize>()
        .map_err(|err| format!("invalid {flag} value '{raw}': {err}"))
}

fn parse_config() -> Result<Config, String> {
    let mut cfg = Config {
        mode: Mode::parse(
            &env::var("AF_SNAPSHOT_TELL_MODE").unwrap_or_else(|_| "all".to_string()),
        )?,
        sync_ops: env::var("AF_SNAPSHOT_SYNC_TELL_OPS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(300_000),
        async_ops: env::var("AF_SNAPSHOT_ASYNC_TELL_OPS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(300_000),
        cap: env::var("AF_SNAPSHOT_MAILBOX_CAP")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(300_000),
        producers: env::var("AF_SNAPSHOT_TELL_PRODUCERS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1),
    };

    let mut it = env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--bench" => {}
            "--help" | "-h" => return Err(usage().to_string()),
            "--mode" => {
                cfg.mode = Mode::parse(
                    &it.next()
                        .ok_or_else(|| "missing value for --mode".to_string())?,
                )?;
            }
            "--sync-ops" => {
                cfg.sync_ops = parse_u64(
                    &it.next()
                        .ok_or_else(|| "missing value for --sync-ops".to_string())?,
                    "--sync-ops",
                )?;
            }
            "--async-ops" => {
                cfg.async_ops = parse_u64(
                    &it.next()
                        .ok_or_else(|| "missing value for --async-ops".to_string())?,
                    "--async-ops",
                )?;
            }
            "--cap" => {
                cfg.cap = parse_usize(
                    &it.next()
                        .ok_or_else(|| "missing value for --cap".to_string())?,
                    "--cap",
                )?;
            }
            "--producers" => {
                cfg.producers = parse_usize(
                    &it.next()
                        .ok_or_else(|| "missing value for --producers".to_string())?,
                    "--producers",
                )?;
            }
            other => return Err(format!("unknown arg: {other}\n\n{}", usage())),
        }
    }

    cfg.sync_ops = cfg.sync_ops.max(1);
    cfg.async_ops = cfg.async_ops.max(1);
    cfg.cap = cfg.cap.max(1);
    cfg.producers = cfg.producers.max(1);
    Ok(cfg)
}

fn split_ops(total: u64, producers: usize) -> Vec<u64> {
    let producers_u64 = producers as u64;
    let base = total / producers_u64;
    let extra = total % producers_u64;
    (0..producers)
        .map(|idx| base + u64::from((idx as u64) < extra))
        .collect()
}

struct SyncTellActor {
    processed: Arc<AtomicU64>,
}

impl icanact_core::runtime_contract::RuntimeContractMarker for SyncTellActor {
    type Contract = icanact_core::runtime_contract::SyncRuntime;
}

impl sync::SyncActor for SyncTellActor {}

impl Tell for SyncTellActor {
    type Msg = u64;

    #[inline(always)]
    fn handle_tell(&mut self, msg: ActorMessage<Self::Msg>) {
        black_box(msg.payload);
        self.processed.fetch_add(1, Ordering::Relaxed);
    }
}

impl sync::SyncEndpoint for SyncTellActor {
    type RuntimeMsg = sync::SyncTellMessage<u64>;

    #[inline(always)]
    fn handle_runtime(&mut self, msg: ActorMessage<Self::RuntimeMsg>) {
        match msg.payload {
            sync::SyncTellMessage::Tell(v) => {
                black_box(v);
                self.processed.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

impl sync::SyncTellEndpoint for SyncTellActor {
    #[inline(always)]
    fn runtime_tell(msg: <Self as Tell>::Msg) -> Self::RuntimeMsg {
        sync::SyncTellMessage::Tell(msg)
    }

    #[inline(always)]
    fn recover_tell(msg: Self::RuntimeMsg) -> Option<<Self as Tell>::Msg> {
        match msg {
            sync::SyncTellMessage::Tell(v) => Some(v),
        }
    }
}

struct ActixSyncTellActor {
    processed: Arc<AtomicU64>,
}

struct ActixTellMsg(u64);

impl ActixMessage for ActixTellMsg {
    type Result = ();
}

impl ActixActor for ActixSyncTellActor {
    type Context = SyncContext<Self>;
}

impl ActixHandler<ActixTellMsg> for ActixSyncTellActor {
    type Result = ();

    #[inline(always)]
    fn handle(&mut self, msg: ActixTellMsg, _ctx: &mut Self::Context) -> Self::Result {
        black_box(msg.0);
        self.processed.fetch_add(1, Ordering::Relaxed);
    }
}

struct AsyncTellActor {
    processed: Arc<AtomicU64>,
}

impl local_async::AsyncActor for AsyncTellActor {}

impl AfAsyncTell for AsyncTellActor {
    type Msg = u64;

    #[inline(always)]
    async fn handle_tell(&mut self, msg: Self::Msg) {
        black_box(msg);
        self.processed.fetch_add(1, Ordering::Relaxed);
    }
}

impl local_async::AsyncEndpoint for AsyncTellActor {
    type RuntimeMsg = u64;

    #[inline(always)]
    async fn handle_runtime(&mut self, msg: Self::RuntimeMsg) {
        <Self as AfAsyncTell>::handle_tell(self, msg).await;
    }
}

impl local_async::AsyncTellEndpoint for AsyncTellActor {
    #[inline(always)]
    fn runtime_tell(msg: <Self as AfAsyncTell>::Msg) -> Self::RuntimeMsg {
        msg
    }

    #[inline(always)]
    fn recover_tell(msg: Self::RuntimeMsg) -> Option<<Self as AfAsyncTell>::Msg> {
        Some(msg)
    }
}

struct ActixAsyncTellActor {
    processed: Arc<AtomicU64>,
    mailbox_cap: usize,
}

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
        black_box(msg.0);
        self.processed.fetch_add(1, Ordering::Relaxed);
    }
}

struct RactorAsyncTellActor;

impl RactorActor for RactorAsyncTellActor {
    type Msg = u64;
    type State = Arc<AtomicU64>;
    type Arguments = Arc<AtomicU64>;

    #[inline(always)]
    fn pre_start(
        &self,
        _myself: RactorActorRef<Self::Msg>,
        processed: Self::Arguments,
    ) -> impl std::future::Future<Output = Result<Self::State, RactorActorProcessingErr>> + Send
    {
        std::future::ready(Ok(processed))
    }

    #[inline(always)]
    fn handle(
        &self,
        _myself: RactorActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> impl std::future::Future<Output = Result<(), RactorActorProcessingErr>> + Send {
        black_box(message);
        state.fetch_add(1, Ordering::Relaxed);
        std::future::ready(Ok(()))
    }
}

#[derive(kameo::Actor)]
struct KameoAsyncTellActor {
    processed: Arc<AtomicU64>,
}

struct KameoTellMsg(u64);

impl KameoMessage<KameoTellMsg> for KameoAsyncTellActor {
    type Reply = ();

    #[inline(always)]
    async fn handle(
        &mut self,
        msg: KameoTellMsg,
        _ctx: &mut KameoContext<Self, Self::Reply>,
    ) -> Self::Reply {
        black_box(msg.0);
        self.processed.fetch_add(1, Ordering::Relaxed);
    }
}

fn run_sync_af(ops: u64, cap: usize, producers: usize) -> f64 {
    let processed = Arc::new(AtomicU64::new(0));
    let (actor_ref, handle) = sync::spawn_with_opts(
        SyncTellActor {
            processed: Arc::clone(&processed),
        },
        sync::SpawnOpts {
            mailbox_capacity: cap,
            ..sync::SpawnOpts::default()
        },
    );
    handle.wait_for_startup();
    let addr = actor_ref
        .snapshot()
        .expect("sync actor ref should expose mailbox snapshot");
    let per_producer = split_ops(ops, producers);

    let warmup = ops.min(10_000);
    for _ in 0..warmup {
        while !addr.tell(sync::SyncTellMessage::Tell(1)) {
            std::hint::spin_loop();
        }
    }
    while processed.load(Ordering::Relaxed) < warmup {
        std::thread::yield_now();
    }

    processed.store(0, Ordering::Relaxed);
    let started = Instant::now();
    let mut joins = Vec::with_capacity(producers);
    for ops in per_producer {
        let addr = addr.clone();
        joins.push(std::thread::spawn(move || {
            for _ in 0..ops {
                while !addr.tell(sync::SyncTellMessage::Tell(1)) {
                    std::hint::spin_loop();
                }
            }
        }));
    }
    for join in joins {
        join.join().expect("sync af producer panicked");
    }
    while processed.load(Ordering::Relaxed) < ops {
        std::thread::yield_now();
    }
    let elapsed = started.elapsed();
    handle.shutdown();
    ops as f64 / elapsed.as_secs_f64()
}

fn run_sync_actix(ops: u64, producers: usize) -> f64 {
    let sys = System::new();
    sys.block_on(async move {
        let processed = Arc::new(AtomicU64::new(0));
        let processed_for_factory = Arc::clone(&processed);
        let addr = SyncArbiter::start(1, move || ActixSyncTellActor {
            processed: Arc::clone(&processed_for_factory),
        });
        let per_producer = split_ops(ops, producers);

        let warmup = ops.min(10_000);
        for _ in 0..warmup {
            addr.do_send(ActixTellMsg(1));
        }
        while processed.load(Ordering::Relaxed) < warmup {
            tokio::task::yield_now().await;
        }

        processed.store(0, Ordering::Relaxed);
        let started = Instant::now();
        let mut joins = Vec::with_capacity(producers);
        for ops in per_producer {
            let addr = addr.clone();
            joins.push(std::thread::spawn(move || {
                for _ in 0..ops {
                    addr.do_send(ActixTellMsg(1));
                }
            }));
        }
        for join in joins {
            join.join().expect("sync actix producer panicked");
        }
        while processed.load(Ordering::Relaxed) < ops {
            tokio::task::yield_now().await;
        }
        ops as f64 / started.elapsed().as_secs_f64()
    })
}

fn run_async_af(ops: u64, cap: usize, producers: usize) -> f64 {
    let mut builder = if producers <= 1 {
        tokio::runtime::Builder::new_current_thread()
    } else {
        let mut b = tokio::runtime::Builder::new_multi_thread();
        b.worker_threads(producers.max(1));
        b
    };
    let rt = builder.enable_all().build().expect("async af runtime");

    rt.block_on(async move {
        let processed = Arc::new(AtomicU64::new(0));
        let (addr, handle) = local_async::spawn_with_opts(
            AsyncTellActor {
                processed: Arc::clone(&processed),
            },
            local_async::SpawnOpts {
                mailbox_capacity: cap,
                ..Default::default()
            },
        );
        let per_producer = split_ops(ops, producers);

        let warmup = ops.min(10_000);
        for _ in 0..warmup {
            assert!(addr.tell(1).await, "async af tell warmup failed");
        }
        while processed.load(Ordering::Relaxed) < warmup {
            tokio::task::yield_now().await;
        }

        processed.store(0, Ordering::Relaxed);
        let started = Instant::now();
        let mut joins = Vec::with_capacity(producers);
        for ops in per_producer {
            let addr = addr.clone();
            joins.push(tokio::spawn(async move {
                for _ in 0..ops {
                    assert!(addr.tell(1).await, "async af tell failed");
                }
            }));
        }
        for join in joins {
            join.await.expect("async af producer panicked");
        }
        while processed.load(Ordering::Relaxed) < ops {
            tokio::task::yield_now().await;
        }
        let out = ops as f64 / started.elapsed().as_secs_f64();
        handle.shutdown().await;
        out
    })
}

fn run_async_actix(ops: u64, cap: usize, producers: usize) -> f64 {
    let sys = System::new();
    sys.block_on(async move {
        let processed = Arc::new(AtomicU64::new(0));
        let addr = ActixAsyncTellActor {
            processed: Arc::clone(&processed),
            mailbox_cap: cap,
        }
        .start();
        let per_producer = split_ops(ops, producers);

        let warmup = ops.min(10_000);
        for _ in 0..warmup {
            addr.do_send(ActixTellMsg(1));
        }
        while processed.load(Ordering::Relaxed) < warmup {
            tokio::task::yield_now().await;
        }

        processed.store(0, Ordering::Relaxed);
        let started = Instant::now();
        let mut joins = Vec::with_capacity(producers);
        for ops in per_producer {
            let addr = addr.clone();
            joins.push(std::thread::spawn(move || {
                for _ in 0..ops {
                    addr.do_send(ActixTellMsg(1));
                }
            }));
        }
        for join in joins {
            join.join().expect("async actix producer panicked");
        }
        while processed.load(Ordering::Relaxed) < ops {
            tokio::task::yield_now().await;
        }
        ops as f64 / started.elapsed().as_secs_f64()
    })
}

fn run_async_ractor(ops: u64, producers: usize) -> f64 {
    let mut builder = if producers <= 1 {
        tokio::runtime::Builder::new_current_thread()
    } else {
        let mut b = tokio::runtime::Builder::new_multi_thread();
        b.worker_threads(producers.max(1));
        b
    };
    let rt = builder.enable_all().build().expect("async ractor runtime");

    rt.block_on(async move {
        let processed = Arc::new(AtomicU64::new(0));
        let (addr, handle) =
            RactorAsyncTellActor::spawn(None, RactorAsyncTellActor, Arc::clone(&processed))
                .await
                .expect("spawn ractor tell actor");
        let per_producer = split_ops(ops, producers);

        let warmup = ops.min(10_000);
        for _ in 0..warmup {
            addr.send_message(1).expect("ractor warmup send failed");
        }
        while processed.load(Ordering::Relaxed) < warmup {
            tokio::task::yield_now().await;
        }

        processed.store(0, Ordering::Relaxed);
        let started = Instant::now();
        let mut joins = Vec::with_capacity(producers);
        for ops in per_producer {
            let addr = addr.clone();
            joins.push(tokio::task::spawn_blocking(move || {
                for _ in 0..ops {
                    addr.send_message(1).expect("ractor send failed");
                }
            }));
        }
        for join in joins {
            join.await.expect("ractor producer panicked");
        }
        while processed.load(Ordering::Relaxed) < ops {
            tokio::task::yield_now().await;
        }
        let out = ops as f64 / started.elapsed().as_secs_f64();
        addr.stop(None);
        handle.await.expect("join ractor tell actor");
        out
    })
}

fn run_async_kameo(ops: u64, cap: usize, producers: usize) -> f64 {
    let mut builder = if producers <= 1 {
        tokio::runtime::Builder::new_current_thread()
    } else {
        let mut b = tokio::runtime::Builder::new_multi_thread();
        b.worker_threads(producers.max(1));
        b
    };
    let rt = builder.enable_all().build().expect("async kameo runtime");

    rt.block_on(async move {
        let processed = Arc::new(AtomicU64::new(0));
        let actor_ref = KameoAsyncTellActor::spawn_with_mailbox(
            KameoAsyncTellActor {
                processed: Arc::clone(&processed),
            },
            mailbox::bounded(cap),
        );
        let per_producer = split_ops(ops, producers);

        let warmup = ops.min(10_000);
        for _ in 0..warmup {
            actor_ref
                .tell(KameoTellMsg(1))
                .send()
                .await
                .expect("kameo warmup send failed");
        }
        while processed.load(Ordering::Relaxed) < warmup {
            tokio::task::yield_now().await;
        }

        processed.store(0, Ordering::Relaxed);
        let started = Instant::now();
        let mut joins = Vec::with_capacity(producers);
        for ops in per_producer {
            let actor_ref = actor_ref.clone();
            joins.push(tokio::spawn(async move {
                for _ in 0..ops {
                    actor_ref
                        .tell(KameoTellMsg(1))
                        .send()
                        .await
                        .expect("kameo send failed");
                }
            }));
        }
        for join in joins {
            join.await.expect("kameo producer panicked");
        }
        while processed.load(Ordering::Relaxed) < ops {
            tokio::task::yield_now().await;
        }
        ops as f64 / started.elapsed().as_secs_f64()
    })
}

fn print_sync(cfg: Config) {
    let af = run_sync_af(cfg.sync_ops, cfg.cap, cfg.producers);
    let actix = run_sync_actix(cfg.sync_ops, cfg.producers);
    println!("bench=framework_tell_snapshot");
    println!("mode=sync");
    println!("ops={}", cfg.sync_ops);
    println!("cap={}", cfg.cap);
    println!("producers={}", cfg.producers);
    println!("icanact_ops_per_sec={af:.3}");
    println!("actix_ops_per_sec={actix:.3}");
    println!("ractor_ops_per_sec=n/a");
    println!("kameo_ops_per_sec=n/a");
}

fn print_async(cfg: Config) {
    let af = run_async_af(cfg.async_ops, cfg.cap, cfg.producers);
    let actix = run_async_actix(cfg.async_ops, cfg.cap, cfg.producers);
    let ractor = run_async_ractor(cfg.async_ops, cfg.producers);
    let kameo = run_async_kameo(cfg.async_ops, cfg.cap, cfg.producers);
    println!("bench=framework_tell_snapshot");
    println!("mode=async");
    println!("ops={}", cfg.async_ops);
    println!("cap={}", cfg.cap);
    println!("producers={}", cfg.producers);
    println!("icanact_ops_per_sec={af:.3}");
    println!("actix_ops_per_sec={actix:.3}");
    println!("ractor_ops_per_sec={ractor:.3}");
    println!("kameo_ops_per_sec={kameo:.3}");
}

fn main() {
    let cfg = match parse_config() {
        Ok(cfg) => cfg,
        Err(err) => {
            eprintln!("{err}");
            std::process::exit(2);
        }
    };

    match cfg.mode {
        Mode::Sync => print_sync(cfg),
        Mode::Async => print_async(cfg),
        Mode::All => {
            print_sync(cfg);
            print_async(cfg);
        }
    }
}
