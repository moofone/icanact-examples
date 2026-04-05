use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::thread;
use std::time::Instant;

use actix::{
    Actor as ActixActor, Context as ActixContext, Handler as ActixHandler,
    Message as ActixMessage, SyncArbiter, SyncContext, System,
};
use icanact_core::local_async as af_async;
use icanact_core::local_async::tell::Tell as AfAsyncTell;
use icanact_core::local_sync as af_sync;
use icanact_core::Tell as AfSyncTell;
use kameo::actor::Spawn as KameoSpawn;
use kameo::mailbox;
use kameo::message::{Context as KameoContext, Message as KameoMessage};
use ractor::{
    Actor as RactorActor, ActorProcessingErr as RactorActorProcessingErr,
    ActorRef as RactorActorRef,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RuntimeKind {
    Sync,
    Async,
    Both,
}

#[derive(Clone, Copy, Debug)]
struct Config {
    runtime: RuntimeKind,
    ops: u64,
    mailbox_cap: usize,
    work_iters: u32,
    runs: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            runtime: RuntimeKind::Both,
            ops: 1_000_000,
            mailbox_cap: 65_536,
            work_iters: 0,
            runs: 3,
        }
    }
}

fn usage() -> &'static str {
    "Usage: cargo bench --bench framework_actor_hotpath -- [options]\n\
     \n\
     Options:\n\
       --runtime sync|async|both  Which runtimes to run (default: both)\n\
       --ops N                    Total messages (default: 1000000)\n\
       --mailbox-cap N            Mailbox capacity (default: 65536)\n\
       --work-iters N             Tiny handler work loop (default: 0)\n\
       --runs N                   Timed runs, median reported (default: 3)\n"
}

fn parse_config() -> Result<Config, String> {
    let mut cfg = Config::default();
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--runtime" => {
                cfg.runtime = match it.next().ok_or_else(|| "missing value for --runtime".to_string())?.as_str() {
                    "sync" => RuntimeKind::Sync,
                    "async" => RuntimeKind::Async,
                    "both" => RuntimeKind::Both,
                    other => return Err(format!("invalid runtime: {other}")),
                };
            }
            "--ops" => {
                cfg.ops = it.next().ok_or_else(|| "missing value for --ops".to_string())?
                    .parse::<u64>().map_err(|_| "invalid integer for --ops".to_string())?.max(1);
            }
            "--mailbox-cap" => {
                cfg.mailbox_cap = it.next().ok_or_else(|| "missing value for --mailbox-cap".to_string())?
                    .parse::<usize>().map_err(|_| "invalid integer for --mailbox-cap".to_string())?.max(1);
            }
            "--work-iters" => {
                cfg.work_iters = it.next().ok_or_else(|| "missing value for --work-iters".to_string())?
                    .parse::<u32>().map_err(|_| "invalid integer for --work-iters".to_string())?;
            }
            "--runs" => {
                cfg.runs = it.next().ok_or_else(|| "missing value for --runs".to_string())?
                    .parse::<usize>().map_err(|_| "invalid integer for --runs".to_string())?.max(1);
            }
            "--help" | "-h" => return Err(usage().to_string()),
            "--bench" => {}
            other => return Err(format!("unknown arg: {other}\n\n{}", usage())),
        }
    }
    Ok(cfg)
}

#[inline(always)]
fn work(mut x: u64, iters: u32) -> u64 {
    let mut acc = x ^ 0x9e37_79b9_7f4a_7c15;
    for _ in 0..iters {
        x = x.wrapping_mul(6364136223846793005).wrapping_add(1);
        acc ^= x.rotate_left(13);
        acc = acc.wrapping_mul(0xff51_afd7_ed55_8ccd).rotate_right(7);
    }
    acc
}

fn expected_checksum(cfg: Config) -> u64 {
    let mut sum = 0u64;
    for token in 0..cfg.ops {
        sum = sum.wrapping_add(work(token, cfg.work_iters));
    }
    sum
}

fn median(values: &mut [f64]) -> f64 {
    values.sort_by(|a, b| a.partial_cmp(b).unwrap());
    values[values.len() / 2]
}

fn spin_then_yield(spins: &mut u32) {
    if *spins < 64 {
        *spins += 1;
        std::hint::spin_loop();
    } else {
        *spins = 0;
        thread::yield_now();
    }
}

#[derive(Clone)]
struct SharedState {
    processed: Arc<AtomicU64>,
    checksum: Arc<AtomicU64>,
    work_iters: u32,
}

impl SharedState {
    fn new(work_iters: u32) -> Self {
        Self {
            processed: Arc::new(AtomicU64::new(0)),
            checksum: Arc::new(AtomicU64::new(0)),
            work_iters,
        }
    }

    fn reset(&self) {
        self.processed.store(0, Ordering::Relaxed);
        self.checksum.store(0, Ordering::Relaxed);
    }

    fn apply(&self, token: u64) {
        let out = work(token, self.work_iters);
        self.processed.fetch_add(1, Ordering::Relaxed);
        self.checksum.fetch_add(out, Ordering::Relaxed);
    }
}

struct AfSyncHotActor {
    state: SharedState,
}

impl af_sync::SyncActor for AfSyncHotActor {}
impl af_sync::SyncEndpoint for AfSyncHotActor {
    type RuntimeMsg = af_sync::SyncTellMessage<u64>;

    fn handle_runtime(&mut self, msg: icanact_core::ActorMessage<Self::RuntimeMsg>) {
        match msg.payload {
            af_sync::SyncTellMessage::Tell(token) => self.state.apply(token),
        }
    }
}
impl af_sync::SyncTellEndpoint for AfSyncHotActor {
    fn runtime_tell(msg: <Self as AfSyncTell>::Msg) -> Self::RuntimeMsg {
        af_sync::SyncTellMessage::Tell(msg)
    }
    fn recover_tell(msg: Self::RuntimeMsg) -> Option<<Self as AfSyncTell>::Msg> {
        match msg { af_sync::SyncTellMessage::Tell(msg) => Some(msg) }
    }
}
impl AfSyncTell for AfSyncHotActor {
    type Msg = u64;
    fn handle_tell(&mut self, msg: icanact_core::ActorMessage<Self::Msg>) {
        self.state.apply(msg.payload);
    }
}

struct ActixSyncHotActor { state: SharedState }
impl ActixActor for ActixSyncHotActor { type Context = SyncContext<Self>; }
#[derive(ActixMessage)] #[rtype(result = "()")] struct ActixSyncTellMsg(u64);
impl ActixHandler<ActixSyncTellMsg> for ActixSyncHotActor {
    type Result = ();
    fn handle(&mut self, msg: ActixSyncTellMsg, _ctx: &mut Self::Context) -> Self::Result { self.state.apply(msg.0); }
}

struct AfAsyncHotActor { state: SharedState }
impl af_async::AsyncActor for AfAsyncHotActor {}
impl af_async::AsyncEndpoint for AfAsyncHotActor {
    type RuntimeMsg = u64;
    async fn handle_runtime(&mut self, token: Self::RuntimeMsg) { self.state.apply(token); }
}
impl af_async::AsyncTellEndpoint for AfAsyncHotActor {
    fn runtime_tell(msg: <Self as AfAsyncTell>::Msg) -> Self::RuntimeMsg { msg }
    fn recover_tell(msg: Self::RuntimeMsg) -> Option<<Self as AfAsyncTell>::Msg> { Some(msg) }
}
impl AfAsyncTell for AfAsyncHotActor {
    type Msg = u64;
    async fn handle_tell(&mut self, token: Self::Msg) { self.state.apply(token); }
}

struct ActixAsyncHotActor { state: SharedState, mailbox_cap: usize }
impl ActixActor for ActixAsyncHotActor {
    type Context = ActixContext<Self>;
    fn started(&mut self, ctx: &mut Self::Context) { ctx.set_mailbox_capacity(self.mailbox_cap); }
}
#[derive(ActixMessage)] #[rtype(result = "()")] struct ActixAsyncTellMsg(u64);
impl ActixHandler<ActixAsyncTellMsg> for ActixAsyncHotActor {
    type Result = ();
    fn handle(&mut self, msg: ActixAsyncTellMsg, _ctx: &mut Self::Context) -> Self::Result { self.state.apply(msg.0); }
}

struct RactorHotActor;
impl RactorActor for RactorHotActor {
    type Msg = u64;
    type State = SharedState;
    type Arguments = SharedState;
    fn pre_start(&self, _myself: RactorActorRef<Self::Msg>, state: Self::Arguments)
        -> impl std::future::Future<Output = Result<Self::State, RactorActorProcessingErr>> + Send {
        std::future::ready(Ok(state))
    }
    fn handle(&self, _myself: RactorActorRef<Self::Msg>, token: Self::Msg, state: &mut Self::State)
        -> impl std::future::Future<Output = Result<(), RactorActorProcessingErr>> + Send {
        state.apply(token);
        std::future::ready(Ok(()))
    }
}

#[derive(kameo::Actor)]
struct KameoHotActor { state: SharedState }
struct KameoTellMsg(u64);
impl KameoMessage<KameoTellMsg> for KameoHotActor {
    type Reply = ();
    async fn handle(&mut self, msg: KameoTellMsg, _ctx: &mut KameoContext<Self, Self::Reply>) -> Self::Reply {
        self.state.apply(msg.0);
    }
}

fn run_af_sync(cfg: Config) -> f64 {
    let expected = expected_checksum(cfg);
    let mut runs = Vec::with_capacity(cfg.runs);
    for _ in 0..cfg.runs {
        let state = SharedState::new(cfg.work_iters);
        let (addr, handle) = af_sync::spawn_with_opts(
            AfSyncHotActor { state: state.clone() },
            af_sync::SpawnOpts { mailbox_capacity: cfg.mailbox_cap, ..Default::default() },
        );
        handle.wait_for_startup();
        state.reset();
        let t0 = Instant::now();
        for token in 0..cfg.ops {
            let mut spins = 0u32;
            while !addr.tell(token) { spin_then_yield(&mut spins); }
        }
        while state.processed.load(Ordering::Relaxed) < cfg.ops {
            let mut spins = 0u32;
            spin_then_yield(&mut spins);
        }
        let elapsed = t0.elapsed();
        assert_eq!(state.checksum.load(Ordering::Relaxed), expected);
        handle.shutdown();
        runs.push(cfg.ops as f64 / elapsed.as_secs_f64());
    }
    median(&mut runs)
}

fn run_actix_sync(cfg: Config) -> f64 {
    let expected = expected_checksum(cfg);
    let mut runs = Vec::with_capacity(cfg.runs);
    for _ in 0..cfg.runs {
        let state = SharedState::new(cfg.work_iters);
        let sys = System::new();
        let tput = sys.block_on(async move {
            let addr = SyncArbiter::start(1, move || ActixSyncHotActor { state: state.clone() });
            state.reset();
            let t0 = Instant::now();
            for token in 0..cfg.ops { addr.do_send(ActixSyncTellMsg(token)); }
            while state.processed.load(Ordering::Relaxed) < cfg.ops { tokio::task::yield_now().await; }
            let elapsed = t0.elapsed();
            assert_eq!(state.checksum.load(Ordering::Relaxed), expected);
            cfg.ops as f64 / elapsed.as_secs_f64()
        });
        runs.push(tput);
    }
    median(&mut runs)
}

fn async_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread().enable_all().build().expect("runtime")
}

fn run_af_async(cfg: Config) -> f64 {
    let expected = expected_checksum(cfg);
    let mut runs = Vec::with_capacity(cfg.runs);
    for _ in 0..cfg.runs {
        let rt = async_runtime();
        let tput = rt.block_on(async move {
            let state = SharedState::new(cfg.work_iters);
            let (addr, handle) = af_async::spawn_with_opts(
                AfAsyncHotActor { state: state.clone() },
                af_async::SpawnOpts { mailbox_capacity: cfg.mailbox_cap, ..Default::default() },
            );
            state.reset();
            let t0 = Instant::now();
            for token in 0..cfg.ops { assert!(addr.tell(token).await); }
            while state.processed.load(Ordering::Relaxed) < cfg.ops { tokio::task::yield_now().await; }
            let elapsed = t0.elapsed();
            assert_eq!(state.checksum.load(Ordering::Relaxed), expected);
            handle.shutdown().await;
            cfg.ops as f64 / elapsed.as_secs_f64()
        });
        runs.push(tput);
    }
    median(&mut runs)
}

fn run_actix_async(cfg: Config) -> f64 {
    let expected = expected_checksum(cfg);
    let mut runs = Vec::with_capacity(cfg.runs);
    for _ in 0..cfg.runs {
        let sys = System::new();
        let tput = sys.block_on(async move {
            let state = SharedState::new(cfg.work_iters);
            let addr = ActixAsyncHotActor { state: state.clone(), mailbox_cap: cfg.mailbox_cap }.start();
            state.reset();
            let t0 = Instant::now();
            for token in 0..cfg.ops { addr.do_send(ActixAsyncTellMsg(token)); }
            while state.processed.load(Ordering::Relaxed) < cfg.ops { tokio::task::yield_now().await; }
            let elapsed = t0.elapsed();
            assert_eq!(state.checksum.load(Ordering::Relaxed), expected);
            cfg.ops as f64 / elapsed.as_secs_f64()
        });
        runs.push(tput);
    }
    median(&mut runs)
}

fn run_ractor_async(cfg: Config) -> f64 {
    let expected = expected_checksum(cfg);
    let mut runs = Vec::with_capacity(cfg.runs);
    for _ in 0..cfg.runs {
        let rt = async_runtime();
        let tput = rt.block_on(async move {
            let state = SharedState::new(cfg.work_iters);
            let (addr, handle) = RactorHotActor::spawn(None, RactorHotActor, state.clone()).await.expect("spawn");
            state.reset();
            let t0 = Instant::now();
            for token in 0..cfg.ops { addr.send_message(token).expect("ractor send failed"); }
            while state.processed.load(Ordering::Relaxed) < cfg.ops { tokio::task::yield_now().await; }
            let elapsed = t0.elapsed();
            assert_eq!(state.checksum.load(Ordering::Relaxed), expected);
            addr.stop(None);
            handle.await.expect("join failed");
            cfg.ops as f64 / elapsed.as_secs_f64()
        });
        runs.push(tput);
    }
    median(&mut runs)
}

fn run_kameo_async(cfg: Config) -> f64 {
    let expected = expected_checksum(cfg);
    let mut runs = Vec::with_capacity(cfg.runs);
    for _ in 0..cfg.runs {
        let rt = async_runtime();
        let tput = rt.block_on(async move {
            let state = SharedState::new(cfg.work_iters);
            let actor_ref = KameoHotActor::spawn_with_mailbox(
                KameoHotActor { state: state.clone() },
                mailbox::bounded(cfg.mailbox_cap),
            );
            state.reset();
            let t0 = Instant::now();
            for token in 0..cfg.ops {
                actor_ref.tell(KameoTellMsg(token)).send().await.expect("kameo send failed");
            }
            while state.processed.load(Ordering::Relaxed) < cfg.ops { tokio::task::yield_now().await; }
            let elapsed = t0.elapsed();
            assert_eq!(state.checksum.load(Ordering::Relaxed), expected);
            cfg.ops as f64 / elapsed.as_secs_f64()
        });
        runs.push(tput);
    }
    median(&mut runs)
}

fn main() {
    let cfg = match parse_config() {
        Ok(cfg) => cfg,
        Err(msg) => { eprintln!("{msg}"); std::process::exit(2); }
    };
    println!("== framework_actor_hotpath ==");
    println!("ops={} mailbox_cap={} work_iters={} runs={}", cfg.ops, cfg.mailbox_cap, cfg.work_iters, cfg.runs);
    match cfg.runtime {
        RuntimeKind::Sync => {
            println!("actor_framework_sync: {:.3} msg/s", run_af_sync(cfg));
            println!("actix_sync: {:.3} msg/s", run_actix_sync(cfg));
            println!("ractor_sync: n/a");
            println!("kameo_sync: n/a");
        }
        RuntimeKind::Async => {
            println!("actor_framework_async: {:.3} msg/s", run_af_async(cfg));
            println!("actix_async: {:.3} msg/s", run_actix_async(cfg));
            println!("ractor_async: {:.3} msg/s", run_ractor_async(cfg));
            println!("kameo_async: {:.3} msg/s", run_kameo_async(cfg));
        }
        RuntimeKind::Both => {
            println!("actor_framework_sync: {:.3} msg/s", run_af_sync(cfg));
            println!("actix_sync: {:.3} msg/s", run_actix_sync(cfg));
            println!("ractor_sync: n/a");
            println!("kameo_sync: n/a");
            println!("actor_framework_async: {:.3} msg/s", run_af_async(cfg));
            println!("actix_async: {:.3} msg/s", run_actix_async(cfg));
            println!("ractor_async: {:.3} msg/s", run_ractor_async(cfg));
            println!("kameo_async: {:.3} msg/s", run_kameo_async(cfg));
        }
    }
}
