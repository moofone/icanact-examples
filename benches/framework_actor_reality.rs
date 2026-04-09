use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::thread;
use std::time::Instant;

use actix::{
    Actor as ActixActor, Context as ActixContext, Handler as ActixHandler, Message as ActixMessage,
    SyncArbiter, SyncContext, System,
};
use icanact_core::local_async as af_async;
use icanact_core::local_sync as af_sync;
use kameo::actor::Spawn as KameoSpawn;
use kameo::mailbox;
use kameo::message::{Context as KameoContext, Message as KameoMessage};
use ractor::{
    Actor as RactorActor, ActorProcessingErr as RactorActorProcessingErr,
    ActorRef as RactorActorRef,
};
use tokio::sync::{mpsc, oneshot};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RuntimeKind {
    Sync,
    Async,
    Both,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Workflow {
    Tell,
    Mixed,
}

#[derive(Clone, Copy, Debug)]
struct Config {
    runtime: RuntimeKind,
    workflow: Workflow,
    producers: usize,
    ops: u64,
    mailbox_cap: usize,
    ask_pct: u32,
    work_iters: u32,
    runs: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            runtime: RuntimeKind::Both,
            workflow: Workflow::Tell,
            producers: 1,
            ops: 200_000,
            mailbox_cap: 65_536,
            ask_pct: 5,
            work_iters: 32,
            runs: 3,
        }
    }
}

fn parse_u64(
    name: &str,
    current: u64,
    it: &mut impl Iterator<Item = String>,
) -> Result<u64, String> {
    it.next()
        .ok_or_else(|| format!("missing value for {name}"))?
        .parse::<u64>()
        .map_err(|_| format!("invalid integer for {name}"))
        .map(|v| v.max(1).max(current.min(1)))
}

fn parse_usize(
    name: &str,
    current: usize,
    it: &mut impl Iterator<Item = String>,
) -> Result<usize, String> {
    it.next()
        .ok_or_else(|| format!("missing value for {name}"))?
        .parse::<usize>()
        .map_err(|_| format!("invalid integer for {name}"))
        .map(|v| v.max(1).max(current.min(1)))
}

fn usage() -> &'static str {
    "Usage: cargo bench --bench framework_actor_reality -- [options]\n\
     \n\
     Options:\n\
       --runtime sync|async|both      Which runtimes to run (default: both)\n\
       --workflow tell|mixed          Tell-only or tell+ask mixed ingress (default: tell)\n\
       --producers N                  Concurrent publishers (default: 1)\n\
       --ops N                        Total operations per run (default: 200000)\n\
       --mailbox-cap N                Mailbox capacity (default: 65536)\n\
       --ask-pct N                    Ask percentage for mixed mode (default: 5)\n\
       --work-iters N                 Handler compute loop iterations (default: 32)\n\
       --runs N                       Timed runs, median reported (default: 3)\n"
}

fn parse_config() -> Result<Config, String> {
    let mut cfg = Config::default();
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--runtime" => {
                cfg.runtime = match it
                    .next()
                    .ok_or_else(|| "missing value for --runtime".to_string())?
                    .as_str()
                {
                    "sync" => RuntimeKind::Sync,
                    "async" => RuntimeKind::Async,
                    "both" => RuntimeKind::Both,
                    other => return Err(format!("invalid runtime: {other}")),
                };
            }
            "--workflow" => {
                cfg.workflow = match it
                    .next()
                    .ok_or_else(|| "missing value for --workflow".to_string())?
                    .as_str()
                {
                    "tell" => Workflow::Tell,
                    "mixed" => Workflow::Mixed,
                    other => return Err(format!("invalid workflow: {other}")),
                };
            }
            "--producers" => cfg.producers = parse_usize("--producers", cfg.producers, &mut it)?,
            "--ops" => cfg.ops = parse_u64("--ops", cfg.ops, &mut it)?,
            "--mailbox-cap" => {
                cfg.mailbox_cap = parse_usize("--mailbox-cap", cfg.mailbox_cap, &mut it)?
            }
            "--ask-pct" => {
                cfg.ask_pct = it
                    .next()
                    .ok_or_else(|| "missing value for --ask-pct".to_string())?
                    .parse::<u32>()
                    .map_err(|_| "invalid integer for --ask-pct".to_string())?
                    .min(100);
            }
            "--work-iters" => {
                cfg.work_iters = it
                    .next()
                    .ok_or_else(|| "missing value for --work-iters".to_string())?
                    .parse::<u32>()
                    .map_err(|_| "invalid integer for --work-iters".to_string())?;
            }
            "--runs" => cfg.runs = parse_usize("--runs", cfg.runs, &mut it)?,
            "--help" | "-h" => return Err(usage().to_string()),
            "--bench" => {}
            other => return Err(format!("unknown arg: {other}\n\n{}", usage())),
        }
    }
    Ok(cfg)
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

    fn apply(&self, token: u64) -> u64 {
        let out = work(token, self.work_iters);
        self.processed.fetch_add(1, Ordering::Relaxed);
        self.checksum.fetch_add(out, Ordering::Relaxed);
        out
    }

    fn take_checksum(&self) -> u64 {
        self.checksum.swap(0, Ordering::Relaxed)
    }
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

#[inline(always)]
fn is_ask(token: u64, ask_pct: u32) -> bool {
    ask_pct != 0 && (token % 100) < ask_pct as u64
}

fn split_ranges(total: u64, producers: usize) -> Vec<(u64, u64)> {
    let base = total / producers as u64;
    let extra = total % producers as u64;
    let mut out = Vec::with_capacity(producers);
    let mut start = 0u64;
    for idx in 0..producers {
        let len = base + u64::from((idx as u64) < extra);
        out.push((start, start + len));
        start += len;
    }
    out
}

fn warmup_ops(total: u64) -> u64 {
    total.min(1_024)
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

#[derive(Clone, Copy)]
struct ResultRow {
    delivered_per_sec: f64,
}

fn print_rows(runtime: &str, cfg: Config, rows: &[(String, Option<ResultRow>)]) {
    println!("== framework_actor_reality ==");
    println!(
        "runtime={} workflow={:?} producers={} ops={} mailbox_cap={} ask_pct={} work_iters={} runs={}",
        runtime,
        cfg.workflow,
        cfg.producers,
        cfg.ops,
        cfg.mailbox_cap,
        cfg.ask_pct,
        cfg.work_iters,
        cfg.runs
    );
    for (name, row) in rows {
        match row {
            Some(row) => println!("{name}: {:.3} msg/s", row.delivered_per_sec),
            None => println!("{name}: n/a"),
        }
    }
}

// ---- sync ----

enum SyncTellMsg {
    Tell(u64),
}

impl icanact_core::TellAskTell for SyncTellMsg {}

enum SyncAskMsg {
    Ask(u64),
}

struct AfSyncRealityActor {
    state: SharedState,
}

impl af_sync::SyncActor for AfSyncRealityActor {
    type Contract = af_sync::contract::TellAsk;
    type Tell = SyncTellMsg;
    type Ask = SyncAskMsg;
    type Reply = u64;
    type Channel = ();
    type PubSub = ();
    type Broadcast = ();

    fn handle_tell(&mut self, msg: Self::Tell) {
        match msg {
            SyncTellMsg::Tell(token) => {
                let _ = self.state.apply(token);
            }
        }
    }

    fn handle_ask(&mut self, msg: Self::Ask) -> Self::Reply {
        match msg {
            SyncAskMsg::Ask(token) => self.state.apply(token),
        }
    }
}

fn warmup_cfg(cfg: Config) -> Config {
    Config {
        ops: warmup_ops(cfg.ops),
        ..cfg
    }
}

struct ActixSyncRealityActor {
    state: SharedState,
}

impl ActixActor for ActixSyncRealityActor {
    type Context = SyncContext<Self>;
}

#[derive(ActixMessage)]
#[rtype(result = "()")]
struct ActixSyncTellMsg(u64);

#[derive(ActixMessage)]
#[rtype(result = "u64")]
struct ActixSyncAskMsg(u64);

impl ActixHandler<ActixSyncTellMsg> for ActixSyncRealityActor {
    type Result = ();

    fn handle(&mut self, msg: ActixSyncTellMsg, _ctx: &mut Self::Context) -> Self::Result {
        let _ = self.state.apply(msg.0);
    }
}

impl ActixHandler<ActixSyncAskMsg> for ActixSyncRealityActor {
    type Result = u64;

    fn handle(&mut self, msg: ActixSyncAskMsg, _ctx: &mut Self::Context) -> Self::Result {
        self.state.apply(msg.0)
    }
}

fn run_af_sync(cfg: Config) -> ResultRow {
    let expected = expected_checksum(cfg);
    let warmup_cfg = warmup_cfg(cfg);
    let warmup_expected = expected_checksum(warmup_cfg);
    let mut runs = Vec::with_capacity(cfg.runs);
    for _ in 0..cfg.runs {
        let state = SharedState::new(cfg.work_iters);
        state.reset();
        let (addr, handle) = af_sync::spawn_with_opts(
            AfSyncRealityActor {
                state: state.clone(),
            },
            af_sync::SpawnOpts {
                mailbox_capacity: cfg.mailbox_cap,
                ..Default::default()
            },
        );
        handle.wait_for_startup();
        for token in 0..warmup_cfg.ops {
            if cfg.workflow == Workflow::Mixed && is_ask(token, cfg.ask_pct) {
                let out = addr
                    .ask(SyncAskMsg::Ask(token))
                    .expect("af sync warmup ask failed");
                assert_eq!(out, work(token, cfg.work_iters));
            } else {
                let mut spins = 0u32;
                while !addr.tell(SyncTellMsg::Tell(token)) {
                    spin_then_yield(&mut spins);
                }
            }
        }
        let mut spins = 0u32;
        while state.processed.load(Ordering::Relaxed) < warmup_cfg.ops {
            spin_then_yield(&mut spins);
        }
        assert_eq!(state.processed.swap(0, Ordering::Relaxed), warmup_cfg.ops);
        assert_eq!(state.take_checksum(), warmup_expected);
        let started = Instant::now();
        let ranges = split_ranges(cfg.ops, cfg.producers);
        let mut joins = Vec::with_capacity(cfg.producers);
        for (start, end) in ranges {
            let addr = addr.clone();
            joins.push(thread::spawn(move || {
                let mut reply_sum = 0u64;
                for token in start..end {
                    if cfg.workflow == Workflow::Mixed && is_ask(token, cfg.ask_pct) {
                        let out = addr
                            .ask(SyncAskMsg::Ask(token))
                            .expect("af sync ask failed");
                        assert_eq!(out, work(token, cfg.work_iters));
                        reply_sum = reply_sum.wrapping_add(out);
                    } else {
                        let mut spins = 0u32;
                        while !addr.tell(SyncTellMsg::Tell(token)) {
                            spin_then_yield(&mut spins);
                        }
                    }
                }
                reply_sum
            }));
        }
        let mut reply_checksum = 0u64;
        for join in joins {
            reply_checksum =
                reply_checksum.wrapping_add(join.join().expect("af sync producer panicked"));
        }
        let mut spins = 0u32;
        while state.processed.load(Ordering::Relaxed) < cfg.ops {
            spin_then_yield(&mut spins);
        }
        let elapsed = started.elapsed();
        assert_eq!(state.processed.swap(0, Ordering::Relaxed), cfg.ops);
        assert_eq!(state.take_checksum(), expected);
        let _ = reply_checksum;
        handle.shutdown();
        runs.push(cfg.ops as f64 / elapsed.as_secs_f64());
    }
    ResultRow {
        delivered_per_sec: median(&mut runs),
    }
}

fn run_actix_sync(cfg: Config) -> ResultRow {
    let expected = expected_checksum(cfg);
    let warmup_cfg = warmup_cfg(cfg);
    let warmup_expected = expected_checksum(warmup_cfg);
    let mut runs = Vec::with_capacity(cfg.runs);
    for _ in 0..cfg.runs {
        let state = SharedState::new(cfg.work_iters);
        state.reset();
        let sys = System::new();
        let throughput = sys.block_on(async move {
            let actor_state = state.clone();
            let addr = SyncArbiter::start(1, move || ActixSyncRealityActor {
                state: actor_state.clone(),
            });
            for token in 0..warmup_cfg.ops {
                if cfg.workflow == Workflow::Mixed && is_ask(token, cfg.ask_pct) {
                    let out = addr
                        .send(ActixSyncAskMsg(token))
                        .await
                        .expect("actix sync warmup ask failed");
                    assert_eq!(out, work(token, cfg.work_iters));
                } else {
                    addr.do_send(ActixSyncTellMsg(token));
                }
            }
            while state.processed.load(Ordering::Relaxed) < warmup_cfg.ops {
                tokio::task::yield_now().await;
            }
            assert_eq!(state.processed.swap(0, Ordering::Relaxed), warmup_cfg.ops);
            assert_eq!(state.take_checksum(), warmup_expected);
            let started = Instant::now();
            let ranges = split_ranges(cfg.ops, cfg.producers);
            let mut joins = Vec::with_capacity(cfg.producers);
            for (start, end) in ranges {
                let addr = addr.clone();
                joins.push(tokio::task::spawn_blocking(move || {
                    let mut reply_sum = 0u64;
                    for token in start..end {
                        if cfg.workflow == Workflow::Mixed && is_ask(token, cfg.ask_pct) {
                            let out =
                                futures::executor::block_on(addr.send(ActixSyncAskMsg(token)))
                                    .expect("actix sync ask failed");
                            assert_eq!(out, work(token, cfg.work_iters));
                            reply_sum = reply_sum.wrapping_add(out);
                        } else {
                            addr.do_send(ActixSyncTellMsg(token));
                        }
                    }
                    reply_sum
                }));
            }
            let mut reply_checksum = 0u64;
            for join in joins {
                reply_checksum =
                    reply_checksum.wrapping_add(join.await.expect("actix sync producer panicked"));
            }
            while state.processed.load(Ordering::Relaxed) < cfg.ops {
                tokio::task::yield_now().await;
            }
            let elapsed = started.elapsed();
            assert_eq!(state.processed.swap(0, Ordering::Relaxed), cfg.ops);
            assert_eq!(state.take_checksum(), expected);
            let _ = reply_checksum;
            cfg.ops as f64 / elapsed.as_secs_f64()
        });
        runs.push(throughput);
    }
    ResultRow {
        delivered_per_sec: median(&mut runs),
    }
}

// ---- async ----

struct AfAsyncRealityActor {
    state: SharedState,
}

impl af_async::AsyncActor for AfAsyncRealityActor {
    type Contract = af_async::contract::TellAsk;
    type Tell = u64;
    type Ask = u64;
    type Reply = u64;
    type Channel = ();
    type PubSub = ();
    type Broadcast = ();

    async fn handle_tell(&mut self, token: Self::Tell) {
        let _ = self.state.apply(token);
    }

    async fn handle_ask(&mut self, token: Self::Ask) -> Self::Reply {
        self.state.apply(token)
    }
}

struct ActixAsyncRealityActor {
    state: SharedState,
    mailbox_cap: usize,
}

enum TokioAsyncRealityMsg {
    Tell(u64),
    Ask(u64, oneshot::Sender<u64>),
}

impl ActixActor for ActixAsyncRealityActor {
    type Context = ActixContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.set_mailbox_capacity(self.mailbox_cap);
    }
}

#[derive(ActixMessage)]
#[rtype(result = "()")]
struct ActixAsyncTellMsg(u64);

#[derive(ActixMessage)]
#[rtype(result = "u64")]
struct ActixAsyncAskMsg(u64);

impl ActixHandler<ActixAsyncTellMsg> for ActixAsyncRealityActor {
    type Result = ();

    fn handle(&mut self, msg: ActixAsyncTellMsg, _ctx: &mut Self::Context) -> Self::Result {
        let _ = self.state.apply(msg.0);
    }
}

impl ActixHandler<ActixAsyncAskMsg> for ActixAsyncRealityActor {
    type Result = u64;

    fn handle(&mut self, msg: ActixAsyncAskMsg, _ctx: &mut Self::Context) -> Self::Result {
        self.state.apply(msg.0)
    }
}

struct RactorTellActor;

impl RactorActor for RactorTellActor {
    type Msg = u64;
    type State = SharedState;
    type Arguments = SharedState;

    fn pre_start(
        &self,
        _myself: RactorActorRef<Self::Msg>,
        state: Self::Arguments,
    ) -> impl std::future::Future<Output = Result<Self::State, RactorActorProcessingErr>> + Send
    {
        std::future::ready(Ok(state))
    }

    fn handle(
        &self,
        _myself: RactorActorRef<Self::Msg>,
        token: Self::Msg,
        state: &mut Self::State,
    ) -> impl std::future::Future<Output = Result<(), RactorActorProcessingErr>> + Send {
        let _ = state.apply(token);
        std::future::ready(Ok(()))
    }
}

#[derive(kameo::Actor)]
struct KameoRealityActor {
    state: SharedState,
}

struct KameoTellMsg(u64);
struct KameoAskMsg(u64);

impl KameoMessage<KameoTellMsg> for KameoRealityActor {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: KameoTellMsg,
        _ctx: &mut KameoContext<Self, Self::Reply>,
    ) -> Self::Reply {
        let _ = self.state.apply(msg.0);
    }
}

impl KameoMessage<KameoAskMsg> for KameoRealityActor {
    type Reply = u64;

    async fn handle(
        &mut self,
        msg: KameoAskMsg,
        _ctx: &mut KameoContext<Self, Self::Reply>,
    ) -> Self::Reply {
        self.state.apply(msg.0)
    }
}

fn async_runtime_builder(workers: usize) -> tokio::runtime::Builder {
    if workers <= 1 {
        tokio::runtime::Builder::new_current_thread()
    } else {
        let mut b = tokio::runtime::Builder::new_multi_thread();
        b.worker_threads(workers);
        b
    }
}

fn run_af_async(cfg: Config) -> ResultRow {
    let expected = expected_checksum(cfg);
    let warmup_cfg = warmup_cfg(cfg);
    let warmup_expected = expected_checksum(warmup_cfg);
    let use_current_runtime = std::env::var("ICANACT_AF_BENCH_CURRENT_THREAD")
        .ok()
        .map(|v| matches!(v.as_str(), "1" | "true" | "yes"))
        .unwrap_or(false);
    let mut runs = Vec::with_capacity(cfg.runs);
    for _ in 0..cfg.runs {
        let rt = async_runtime_builder(if use_current_runtime {
            1
        } else {
            cfg.producers.max(1)
        })
        .enable_all()
        .build()
        .expect("af async runtime");
        let throughput = rt.block_on(async move {
            let state = SharedState::new(cfg.work_iters);
            state.reset();
            let (addr, handle) = af_async::spawn_with_opts(
                AfAsyncRealityActor {
                    state: state.clone(),
                },
                af_async::SpawnOpts {
                    mailbox_capacity: cfg.mailbox_cap,
                    ..Default::default()
                },
            )
            .await;
            let resolved = addr
                .resolve_tell_fast()
                .expect("af async resolved tell ref");
            for token in 0..warmup_cfg.ops {
                if cfg.workflow == Workflow::Mixed && is_ask(token, cfg.ask_pct) {
                    let out = addr.ask(token).await.expect("af async warmup ask failed");
                    assert_eq!(out, work(token, cfg.work_iters));
                } else {
                    let mut warmup_addr = resolved.clone();
                    while !warmup_addr.tell(token) {
                        tokio::task::yield_now().await;
                    }
                }
            }
            while state.processed.load(Ordering::Relaxed) < warmup_cfg.ops {
                tokio::task::yield_now().await;
            }
            assert_eq!(state.processed.swap(0, Ordering::Relaxed), warmup_cfg.ops);
            assert_eq!(state.take_checksum(), warmup_expected);
            let started = Instant::now();
            let ranges = split_ranges(cfg.ops, cfg.producers);
            let mut joins = Vec::with_capacity(cfg.producers);
            for (start, end) in ranges {
                let addr = addr.clone();
                let mut resolved = resolved.clone();
                joins.push(tokio::spawn(async move {
                    let mut reply_sum = 0u64;
                    for token in start..end {
                        if cfg.workflow == Workflow::Mixed && is_ask(token, cfg.ask_pct) {
                            let out = addr.ask(token).await.expect("af async ask failed");
                            assert_eq!(out, work(token, cfg.work_iters));
                            reply_sum = reply_sum.wrapping_add(out);
                        } else {
                            while !resolved.tell(token) {
                                tokio::task::yield_now().await;
                            }
                        }
                    }
                    reply_sum
                }));
            }
            let mut reply_checksum = 0u64;
            for join in joins {
                reply_checksum =
                    reply_checksum.wrapping_add(join.await.expect("af async producer panicked"));
            }
            while state.processed.load(Ordering::Relaxed) < cfg.ops {
                tokio::task::yield_now().await;
            }
            let elapsed = started.elapsed();
            assert_eq!(state.processed.swap(0, Ordering::Relaxed), cfg.ops);
            assert_eq!(state.take_checksum(), expected);
            let _ = reply_checksum;
            handle.shutdown().await;
            cfg.ops as f64 / elapsed.as_secs_f64()
        });
        runs.push(throughput);
    }
    ResultRow {
        delivered_per_sec: median(&mut runs),
    }
}

fn run_actix_async(cfg: Config) -> ResultRow {
    let expected = expected_checksum(cfg);
    let warmup_cfg = warmup_cfg(cfg);
    let warmup_expected = expected_checksum(warmup_cfg);
    let mut runs = Vec::with_capacity(cfg.runs);
    for _ in 0..cfg.runs {
        let sys = System::new();
        let throughput = sys.block_on(async move {
            let state = SharedState::new(cfg.work_iters);
            state.reset();
            let addr = ActixAsyncRealityActor {
                state: state.clone(),
                mailbox_cap: cfg.mailbox_cap,
            }
            .start();
            for token in 0..warmup_cfg.ops {
                if cfg.workflow == Workflow::Mixed && is_ask(token, cfg.ask_pct) {
                    let out = addr
                        .send(ActixAsyncAskMsg(token))
                        .await
                        .expect("actix async warmup ask failed");
                    assert_eq!(out, work(token, cfg.work_iters));
                } else {
                    addr.do_send(ActixAsyncTellMsg(token));
                }
            }
            while state.processed.load(Ordering::Relaxed) < warmup_cfg.ops {
                tokio::task::yield_now().await;
            }
            assert_eq!(state.processed.swap(0, Ordering::Relaxed), warmup_cfg.ops);
            assert_eq!(state.take_checksum(), warmup_expected);
            let started = Instant::now();
            let ranges = split_ranges(cfg.ops, cfg.producers);
            let mut joins = Vec::with_capacity(cfg.producers);
            for (start, end) in ranges {
                let addr = addr.clone();
                joins.push(tokio::spawn(async move {
                    let mut reply_sum = 0u64;
                    for token in start..end {
                        if cfg.workflow == Workflow::Mixed && is_ask(token, cfg.ask_pct) {
                            let out = addr
                                .send(ActixAsyncAskMsg(token))
                                .await
                                .expect("actix async ask failed");
                            assert_eq!(out, work(token, cfg.work_iters));
                            reply_sum = reply_sum.wrapping_add(out);
                        } else {
                            addr.do_send(ActixAsyncTellMsg(token));
                        }
                    }
                    reply_sum
                }));
            }
            let mut reply_checksum = 0u64;
            for join in joins {
                reply_checksum =
                    reply_checksum.wrapping_add(join.await.expect("actix async producer panicked"));
            }
            while state.processed.load(Ordering::Relaxed) < cfg.ops {
                tokio::task::yield_now().await;
            }
            let elapsed = started.elapsed();
            assert_eq!(state.processed.swap(0, Ordering::Relaxed), cfg.ops);
            assert_eq!(state.take_checksum(), expected);
            let _ = reply_checksum;
            cfg.ops as f64 / elapsed.as_secs_f64()
        });
        runs.push(throughput);
    }
    ResultRow {
        delivered_per_sec: median(&mut runs),
    }
}

fn run_tokio_mpsc_async(cfg: Config) -> ResultRow {
    let expected = expected_checksum(cfg);
    let use_await_tell = std::env::var("ICANACT_AF_AWAIT_TELL")
        .ok()
        .map(|v| matches!(v.as_str(), "1" | "true" | "yes"))
        .unwrap_or(false);
    let use_current_runtime = std::env::var("ICANACT_AF_BENCH_CURRENT_THREAD")
        .ok()
        .map(|v| matches!(v.as_str(), "1" | "true" | "yes"))
        .unwrap_or(false);
    let mut runs = Vec::with_capacity(cfg.runs);
    for _ in 0..cfg.runs {
        let rt = async_runtime_builder(if use_current_runtime {
            1
        } else {
            cfg.producers.max(1)
        })
        .enable_all()
        .build()
        .expect("tokio mpsc async runtime");
        let throughput = rt.block_on(async move {
            let state = SharedState::new(cfg.work_iters);
            state.reset();
            let started = Instant::now();
            let (tx, mut rx) = mpsc::channel::<TokioAsyncRealityMsg>(cfg.mailbox_cap.max(1));
            let actor_state = state.clone();
            let actor = tokio::spawn(async move {
                while let Some(msg) = rx.recv().await {
                    match msg {
                        TokioAsyncRealityMsg::Tell(token) => {
                            let _ = actor_state.apply(token);
                        }
                        TokioAsyncRealityMsg::Ask(token, reply) => {
                            let _ = reply.send(actor_state.apply(token));
                        }
                    }
                }
            });
            let ranges = split_ranges(cfg.ops, cfg.producers);
            let mut joins = Vec::with_capacity(cfg.producers);
            for (start, end) in ranges {
                let tx = tx.clone();
                joins.push(tokio::spawn(async move {
                    let mut reply_sum = 0u64;
                    for token in start..end {
                        if cfg.workflow == Workflow::Mixed && is_ask(token, cfg.ask_pct) {
                            let (reply_tx, reply_rx) = oneshot::channel();
                            let mut msg = TokioAsyncRealityMsg::Ask(token, reply_tx);
                            loop {
                                match tx.try_send(msg) {
                                    Ok(()) => break,
                                    Err(mpsc::error::TrySendError::Full(returned)) => {
                                        msg = returned;
                                        tokio::task::yield_now().await;
                                    }
                                    Err(mpsc::error::TrySendError::Closed(_)) => {
                                        panic!("tokio mpsc ask failed: closed");
                                    }
                                }
                            }
                            let out = reply_rx.await.expect("tokio mpsc ask reply failed");
                            assert_eq!(out, work(token, cfg.work_iters));
                            reply_sum = reply_sum.wrapping_add(out);
                        } else if use_await_tell {
                            tx.send(TokioAsyncRealityMsg::Tell(token))
                                .await
                                .expect("tokio mpsc tell failed");
                        } else {
                            let mut msg = TokioAsyncRealityMsg::Tell(token);
                            loop {
                                match tx.try_send(msg) {
                                    Ok(()) => break,
                                    Err(mpsc::error::TrySendError::Full(returned)) => {
                                        msg = returned;
                                        tokio::task::yield_now().await;
                                    }
                                    Err(mpsc::error::TrySendError::Closed(_)) => {
                                        panic!("tokio mpsc tell failed: closed");
                                    }
                                }
                            }
                        }
                    }
                    reply_sum
                }));
            }
            drop(tx);
            let mut reply_checksum = 0u64;
            for join in joins {
                reply_checksum =
                    reply_checksum.wrapping_add(join.await.expect("tokio mpsc producer panicked"));
            }
            while state.processed.load(Ordering::Relaxed) < cfg.ops {
                tokio::task::yield_now().await;
            }
            actor.await.expect("tokio mpsc actor panicked");
            let elapsed = started.elapsed();
            let observed = state.checksum.load(Ordering::Relaxed);
            assert_eq!(observed, expected);
            let _ = reply_checksum;
            cfg.ops as f64 / elapsed.as_secs_f64()
        });
        runs.push(throughput);
    }
    ResultRow {
        delivered_per_sec: median(&mut runs),
    }
}

fn run_ractor_async_tell(cfg: Config) -> ResultRow {
    let expected = expected_checksum(cfg);
    let mut runs = Vec::with_capacity(cfg.runs);
    for _ in 0..cfg.runs {
        let rt = async_runtime_builder(cfg.producers.max(1))
            .enable_all()
            .build()
            .expect("ractor async runtime");
        let throughput = rt.block_on(async move {
            let state = SharedState::new(cfg.work_iters);
            state.reset();
            let started = Instant::now();
            let mut addrs = Vec::with_capacity(1);
            let mut handles = Vec::with_capacity(1);
            for _ in 0..1 {
                let (addr, handle) = RactorTellActor::spawn(None, RactorTellActor, state.clone())
                    .await
                    .expect("spawn ractor actor");
                addrs.push(addr);
                handles.push(handle);
            }
            let ranges = split_ranges(cfg.ops, cfg.producers);
            let mut joins = Vec::with_capacity(cfg.producers);
            for (start, end) in ranges {
                let addrs = addrs.clone();
                joins.push(tokio::task::spawn_blocking(move || {
                    for token in start..end {
                        let shard = (token as usize) % addrs.len();
                        addrs[shard]
                            .send_message(token)
                            .expect("ractor tell failed");
                    }
                }));
            }
            for join in joins {
                join.await.expect("ractor producer panicked");
            }
            while state.processed.load(Ordering::Relaxed) < cfg.ops {
                tokio::task::yield_now().await;
            }
            let elapsed = started.elapsed();
            let observed = state.checksum.load(Ordering::Relaxed);
            assert_eq!(observed, expected);
            for addr in addrs {
                addr.stop(None);
            }
            for handle in handles {
                handle.await.expect("ractor actor join failed");
            }
            cfg.ops as f64 / elapsed.as_secs_f64()
        });
        runs.push(throughput);
    }
    ResultRow {
        delivered_per_sec: median(&mut runs),
    }
}

fn run_kameo_async(cfg: Config) -> ResultRow {
    let expected = expected_checksum(cfg);
    let warmup_cfg = warmup_cfg(cfg);
    let warmup_expected = expected_checksum(warmup_cfg);
    let mut runs = Vec::with_capacity(cfg.runs);
    for _ in 0..cfg.runs {
        let rt = async_runtime_builder(cfg.producers.max(1))
            .enable_all()
            .build()
            .expect("kameo async runtime");
        let throughput = rt.block_on(async move {
            let state = SharedState::new(cfg.work_iters);
            state.reset();
            let addr = KameoRealityActor::spawn_with_mailbox(
                KameoRealityActor {
                    state: state.clone(),
                },
                mailbox::bounded(cfg.mailbox_cap),
            );
            for token in 0..warmup_cfg.ops {
                if cfg.workflow == Workflow::Mixed && is_ask(token, cfg.ask_pct) {
                    let out = addr
                        .ask(KameoAskMsg(token))
                        .send()
                        .await
                        .expect("kameo warmup ask failed");
                    assert_eq!(out, work(token, cfg.work_iters));
                } else {
                    addr.tell(KameoTellMsg(token))
                        .send()
                        .await
                        .expect("kameo warmup tell failed");
                }
            }
            while state.processed.load(Ordering::Relaxed) < warmup_cfg.ops {
                tokio::task::yield_now().await;
            }
            assert_eq!(state.processed.swap(0, Ordering::Relaxed), warmup_cfg.ops);
            assert_eq!(state.take_checksum(), warmup_expected);
            let started = Instant::now();
            let ranges = split_ranges(cfg.ops, cfg.producers);
            let mut joins = Vec::with_capacity(cfg.producers);
            for (start, end) in ranges {
                let addr = addr.clone();
                joins.push(tokio::spawn(async move {
                    let mut reply_sum = 0u64;
                    for token in start..end {
                        if cfg.workflow == Workflow::Mixed && is_ask(token, cfg.ask_pct) {
                            let out = addr
                                .ask(KameoAskMsg(token))
                                .send()
                                .await
                                .expect("kameo ask failed");
                            assert_eq!(out, work(token, cfg.work_iters));
                            reply_sum = reply_sum.wrapping_add(out);
                        } else {
                            addr.tell(KameoTellMsg(token))
                                .send()
                                .await
                                .expect("kameo tell failed");
                        }
                    }
                    reply_sum
                }));
            }
            let mut reply_checksum = 0u64;
            for join in joins {
                reply_checksum =
                    reply_checksum.wrapping_add(join.await.expect("kameo producer panicked"));
            }
            while state.processed.load(Ordering::Relaxed) < cfg.ops {
                tokio::task::yield_now().await;
            }
            let elapsed = started.elapsed();
            assert_eq!(state.processed.swap(0, Ordering::Relaxed), cfg.ops);
            assert_eq!(state.take_checksum(), expected);
            let _ = reply_checksum;
            cfg.ops as f64 / elapsed.as_secs_f64()
        });
        runs.push(throughput);
    }
    ResultRow {
        delivered_per_sec: median(&mut runs),
    }
}

fn main() {
    let cfg = match parse_config() {
        Ok(cfg) => cfg,
        Err(msg) => {
            eprintln!("{msg}");
            std::process::exit(2);
        }
    };

    match cfg.runtime {
        RuntimeKind::Sync => {
            let rows = vec![
                ("actor_framework_sync".to_string(), Some(run_af_sync(cfg))),
                ("actix_sync".to_string(), Some(run_actix_sync(cfg))),
                ("ractor_sync".to_string(), None),
                ("kameo_sync".to_string(), None),
            ];
            print_rows("sync", cfg, &rows);
        }
        RuntimeKind::Async => {
            let mut rows = vec![
                ("actor_framework_async".to_string(), Some(run_af_async(cfg))),
                (
                    "pure_tokio_mpsc_async".to_string(),
                    Some(run_tokio_mpsc_async(cfg)),
                ),
                ("actix_async".to_string(), Some(run_actix_async(cfg))),
            ];
            if cfg.workflow == Workflow::Tell {
                rows.push(("ractor_async".to_string(), Some(run_ractor_async_tell(cfg))));
            } else {
                rows.push(("ractor_async".to_string(), None));
            }
            rows.push(("kameo_async".to_string(), Some(run_kameo_async(cfg))));
            print_rows("async", cfg, &rows);
        }
        RuntimeKind::Both => {
            let sync_rows = vec![
                ("actor_framework_sync".to_string(), Some(run_af_sync(cfg))),
                ("actix_sync".to_string(), Some(run_actix_sync(cfg))),
                ("ractor_sync".to_string(), None),
                ("kameo_sync".to_string(), None),
            ];
            print_rows("sync", cfg, &sync_rows);
            let mut async_rows = vec![
                ("actor_framework_async".to_string(), Some(run_af_async(cfg))),
                (
                    "pure_tokio_mpsc_async".to_string(),
                    Some(run_tokio_mpsc_async(cfg)),
                ),
                ("actix_async".to_string(), Some(run_actix_async(cfg))),
            ];
            if cfg.workflow == Workflow::Tell {
                async_rows.push(("ractor_async".to_string(), Some(run_ractor_async_tell(cfg))));
            } else {
                async_rows.push(("ractor_async".to_string(), None));
            }
            async_rows.push(("kameo_async".to_string(), Some(run_kameo_async(cfg))));
            print_rows("async", cfg, &async_rows);
        }
    }
}
