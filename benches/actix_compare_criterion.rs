use actix::prelude::SendError as ActixSendError;
use actix::{
    Actor as ActixActor, ActorContext as _, Context as ActixContext, Handler as ActixHandler,
    Message as ActixMessage, SyncArbiter, SyncContext, System,
};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use icanact_core::{
    local_async, local_direct as direct, local_sync as sync,
};
use icanact_core::local_async::{AsyncActor as AfAsyncActor, AsyncContext as AfAsyncContext};
use icanact_core::local_sync::SyncActor as AfActor;
use icanact_core::{Ask as AfAsk, Tell as AfTell};
use ractor::port::OutputPort as RactorOutputPort;
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
    try_tell_ops: u64,
    ask_ops: u64,
    spawn_ops: u64,
    ephemeral_ops: u64,
    async_fanout_actors: u64,
    async_io_units: u64,
    async_io_sleep_us: u64,
    async_work_iters: u64,
    async_runtime_threads: usize,
    sync_work_ops: u64,
    sync_work_iters: u64,
    sync_work_shards: usize,
    pubsub_topics: usize,
    pubsub_subs_per_topic: usize,
    pubsub_msgs: u64,
    pubsub_warmup: u64,
    pubsub_mailbox_cap: usize,
    pubsub_runtime_threads: usize,
    pubsub_hot_topic: bool,
    pubsub_stripes: usize,
    pubsub_threshold: usize,
    broadcast_subs: usize,
    broadcast_msgs: u64,
    broadcast_warmup: u64,
    broadcast_mailbox_cap: usize,
    broadcast_stripes: usize,
    broadcast_threshold: usize,
    sync_mailbox_cap: usize,
    async_mailbox_cap: usize,
    async_work_mailbox_cap: usize,
    spawn_mailbox_cap: usize,
    actix_sync_threads: usize,
    af_sync_pool_size: usize,
    af_sync_pool_spin_init: usize,
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

fn parse_bool_env(key: &str, default: bool) -> bool {
    match std::env::var(key) {
        Ok(v) => match v.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => true,
            "0" | "false" | "no" | "off" => false,
            _ => default,
        },
        Err(_) => default,
    }
}

impl Config {
    fn from_env() -> Self {
        let try_tell_ops = parse_u64_env("AF_CMP_TRY_TELL_OPS", 1_000_000);
        let ask_ops = parse_u64_env("AF_CMP_ASK_OPS", 200_000);
        let spawn_ops = parse_u64_env("AF_CMP_SPAWN_OPS", 50_000).max(1);
        let ephemeral_ops = parse_u64_env("AF_CMP_EPHEMERAL_OPS", 1_000).max(1);
        let async_fanout_actors = parse_u64_env("AF_CMP_ASYNC_FANOUT_ACTORS", 1_000).max(1);
        let async_io_units = parse_u64_env("AF_CMP_ASYNC_IO_UNITS", 1_000).max(1);
        let async_io_sleep_us = parse_u64_env("AF_CMP_ASYNC_IO_SLEEP_US", 1_000).max(1);
        let async_work_iters = parse_u64_env("AF_CMP_ASYNC_WORK_ITERS", 256).max(1);
        let async_runtime_threads = parse_usize_env("AF_CMP_ASYNC_RT_THREADS", 4).max(1);
        let sync_work_ops = parse_u64_env("AF_CMP_SYNC_WORK_OPS", 1_000_000).max(1);
        let sync_work_iters = parse_u64_env("AF_CMP_SYNC_WORK_ITERS", 256).max(1);
        let pubsub_topics = parse_usize_env("AF_CMP_PUBSUB_TOPICS", 64).max(1);
        let pubsub_subs_per_topic = parse_usize_env("AF_CMP_PUBSUB_SUBS_PER_TOPIC", 256).max(1);
        let pubsub_msgs = parse_u64_env("AF_CMP_PUBSUB_MSGS", 200_000).max(1);
        let pubsub_warmup = parse_u64_env("AF_CMP_PUBSUB_WARMUP", 20_000);
        let pubsub_mailbox_cap = parse_usize_env("AF_CMP_PUBSUB_MAILBOX_CAP", 65_536).max(1);
        let pubsub_runtime_threads = parse_usize_env("AF_CMP_PUBSUB_RT_THREADS", 2).max(1);
        let pubsub_stripes = parse_usize_env(
            "AF_CMP_PUBSUB_STRIPES",
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1),
        )
        .max(1);
        let pubsub_threshold = parse_usize_env("AF_CMP_PUBSUB_THRESHOLD", 1).max(1);
        let broadcast_subs = parse_usize_env("AF_CMP_BROADCAST_SUBS", 256).max(1);
        let broadcast_msgs = parse_u64_env("AF_CMP_BROADCAST_MSGS", 10_000).max(1);
        let broadcast_warmup = parse_u64_env("AF_CMP_BROADCAST_WARMUP", 1_000);
        let broadcast_mailbox_cap = parse_usize_env("AF_CMP_BROADCAST_MAILBOX_CAP", 65_536).max(1);
        let broadcast_stripes = parse_usize_env(
            "AF_CMP_BROADCAST_STRIPES",
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1),
        )
        .max(1);
        let broadcast_threshold = parse_usize_env("AF_CMP_BROADCAST_THRESHOLD", 1).max(1);
        let sample_size = parse_usize_env("AF_CMP_SAMPLE_SIZE", 10).clamp(10, 200);
        let actix_sync_threads = parse_usize_env("AF_CMP_ACTIX_SYNC_THREADS", 1).max(1);
        let af_sync_pool_size =
            parse_usize_env("AF_CMP_AF_SYNC_POOL_SIZE", actix_sync_threads).max(1);
        let sync_work_shards = parse_usize_env("AF_CMP_SYNC_WORK_SHARDS", af_sync_pool_size)
            .max(1)
            .min(af_sync_pool_size);
        let mailbox_cap_legacy = parse_usize_env("AF_CMP_MAILBOX_CAP", 65_536).max(1);
        let sync_cap_default = usize::try_from(try_tell_ops)
            .unwrap_or(usize::MAX.saturating_sub(1))
            .max(2);
        let async_work_ops_usize = usize::try_from(sync_work_ops)
            .unwrap_or(usize::MAX.saturating_sub(1))
            .max(1);
        let async_work_cap_default = async_work_ops_usize
            .saturating_mul(5)
            .saturating_div(2)
            .max(mailbox_cap_legacy)
            .max(async_work_ops_usize);
        Self {
            try_tell_ops,
            ask_ops,
            spawn_ops,
            ephemeral_ops,
            async_fanout_actors,
            async_io_units,
            async_io_sleep_us,
            async_work_iters,
            async_runtime_threads,
            sync_work_ops,
            sync_work_iters,
            sync_work_shards,
            pubsub_topics,
            pubsub_subs_per_topic,
            pubsub_msgs,
            pubsub_warmup,
            pubsub_mailbox_cap,
            pubsub_runtime_threads,
            pubsub_hot_topic: parse_bool_env("AF_CMP_PUBSUB_HOT_TOPIC", false),
            pubsub_stripes,
            pubsub_threshold,
            broadcast_subs,
            broadcast_msgs,
            broadcast_warmup,
            broadcast_mailbox_cap,
            broadcast_stripes,
            broadcast_threshold,
            // Actix SyncArbiter uses channel(0), which behaves effectively unbounded.
            // Default ours to >= ops so sync tell doesn't get throttled by backpressure mismatch.
            sync_mailbox_cap: parse_usize_env("AF_CMP_SYNC_MAILBOX_CAP", sync_cap_default).max(1),
            async_mailbox_cap: parse_usize_env("AF_CMP_ASYNC_MAILBOX_CAP", mailbox_cap_legacy)
                .max(1),
            async_work_mailbox_cap: parse_usize_env(
                "AF_CMP_ASYNC_WORK_MAILBOX_CAP",
                async_work_cap_default,
            )
            .max(1),
            spawn_mailbox_cap: parse_usize_env("AF_CMP_SPAWN_MAILBOX_CAP", 64).max(1),
            actix_sync_threads,
            af_sync_pool_size,
            af_sync_pool_spin_init: parse_usize_env("AF_CMP_AF_SYNC_POOL_SPIN_INIT", 64),
            sample_size,
            warmup_ms: parse_u64_env("AF_CMP_WARMUP_MS", 500),
            measurement_ms: parse_u64_env("AF_CMP_MEASUREMENT_MS", 1_500),
        }
    }
}

struct AfSyncTryTellActor {
    processed: Arc<AtomicU64>,
}

impl AfActor for AfSyncTryTellActor {
    #[inline(always)]
    fn handle_tell(&mut self, _msg: icanact_core::ActorMessage<<Self as AfTell>::Msg>) {
        self.processed.fetch_add(1, Ordering::Relaxed);
    }
}

impl AfTell for AfSyncTryTellActor {
    type Msg = u64;
}

enum AfSyncAskMsg {
    Echo { v: u64, reply: sync::ReplyTo<u64> },
}

struct AfSyncAskActor;

impl AfActor for AfSyncAskActor {
    #[inline(always)]
    fn handle_tell(&mut self, msg: icanact_core::ActorMessage<<Self as AfTell>::Msg>) {
        match msg.payload {
            AfSyncAskMsg::Echo { v, reply } => {
                let _ = reply.reply(v);
            }
        }
    }
}

impl AfTell for AfSyncAskActor {
    type Msg = AfSyncAskMsg;
}

impl AfAsk for AfSyncAskActor {
    type Msg = AfSyncAskMsg;
    type Reply = u64;

    #[inline(always)]
    fn handle_ask(&mut self, msg: icanact_core::ActorMessage<Self::Msg>) -> Self::Reply {
        match msg.payload {
            AfSyncAskMsg::Echo { v, reply } => {
                let _ = reply.reply(v);
                v
            }
        }
    }
}

struct AfSyncSpawnActor {
    started_token: Arc<AtomicU64>,
    token: u64,
}

impl AfActor for AfSyncSpawnActor {
    #[inline(always)]
    fn on_start(&mut self) {
        self.started_token.store(self.token, Ordering::Release);
    }

    #[inline(always)]
    fn handle_tell(&mut self, _msg: icanact_core::ActorMessage<<Self as AfTell>::Msg>) {}
}

impl AfTell for AfSyncSpawnActor {
    type Msg = ();
}

struct AfSyncEphemeralActor {
    processed: Arc<AtomicU64>,
}

impl AfActor for AfSyncEphemeralActor {
    #[inline(always)]
    fn handle_tell(&mut self, msg: icanact_core::ActorMessage<<Self as AfTell>::Msg>) {
        self.processed.store(black_box(msg.payload), Ordering::Release);
    }
}

impl AfTell for AfSyncEphemeralActor {
    type Msg = u64;
}

struct AfSyncWorkActor {
    processed: Arc<AtomicU64>,
    iters: u32,
}

impl AfActor for AfSyncWorkActor {
    #[inline(always)]
    fn handle_tell(&mut self, _msg: icanact_core::ActorMessage<<Self as AfTell>::Msg>) {
        black_box(run_micro_compute(self.iters));
        self.processed.fetch_add(1, Ordering::Release);
    }
}

impl AfTell for AfSyncWorkActor {
    type Msg = ();
}

enum AfSyncEphemeralAskMsg {
    Compute {
        token: u64,
        reply: sync::ReplyTo<u64>,
    },
}

struct AfSyncEphemeralAskActor;

impl AfActor for AfSyncEphemeralAskActor {
    #[inline(always)]
    fn handle_tell(&mut self, msg: icanact_core::ActorMessage<<Self as AfTell>::Msg>) {
        match msg.payload {
            AfSyncEphemeralAskMsg::Compute { token, reply } => {
                let _ = reply.reply(black_box(token).wrapping_add(1));
            }
        }
    }
}

impl AfTell for AfSyncEphemeralAskActor {
    type Msg = AfSyncEphemeralAskMsg;
}

impl AfAsk for AfSyncEphemeralAskActor {
    type Msg = AfSyncEphemeralAskMsg;
    type Reply = u64;

    #[inline(always)]
    fn handle_ask(&mut self, msg: icanact_core::ActorMessage<Self::Msg>) -> Self::Reply {
        match msg.payload {
            AfSyncEphemeralAskMsg::Compute { token, reply } => {
                let out = black_box(token).wrapping_add(1);
                let _ = reply.reply(out);
                out
            }
        }
    }
}

struct AfAsyncTryTellActor {
    processed: Arc<AtomicU64>,
}

impl AfAsyncActor for AfAsyncTryTellActor {
    type Msg = u64;

    #[inline(always)]
    fn handle_tell(&mut self, _msg: Self::Msg) -> impl std::future::Future<Output = ()> + Send {
        self.processed.fetch_add(1, Ordering::Relaxed);
        std::future::ready(())
    }
}

enum AfAsyncAskMsg {
    Echo {
        v: u64,
        reply: local_async::ReplyTo<u64>,
    },
}

struct AfAsyncAskActor;

impl AfAsyncActor for AfAsyncAskActor {
    type Msg = AfAsyncAskMsg;

    #[inline(always)]
    fn handle_tell(&mut self, msg: Self::Msg) -> impl std::future::Future<Output = ()> + Send {
        match msg {
            AfAsyncAskMsg::Echo { v, reply } => {
                let _ = reply.reply(v);
            }
        }
        std::future::ready(())
    }
}

enum AfAsyncFanoutMsg {
    Close,
}

struct AfAsyncFanoutActor {
    stopped: Arc<AtomicU64>,
}

impl AfAsyncActor for AfAsyncFanoutActor {
    type Msg = AfAsyncFanoutMsg;
    const USES_ASYNC_CONTEXT: bool = true;

    #[inline(always)]
    fn handle_tell(&mut self, msg: Self::Msg) -> impl std::future::Future<Output = ()> + Send {
        match msg {
            AfAsyncFanoutMsg::Close => {}
        }
        std::future::ready(())
    }

    #[inline(always)]
    fn handle_tell_with_ctx(
        &mut self,
        msg: Self::Msg,
        ctx: &mut AfAsyncContext,
    ) -> impl std::future::Future<Output = ()> + Send {
        match msg {
            AfAsyncFanoutMsg::Close => {}
        }
        ctx.request_shutdown();
        std::future::ready(())
    }

    #[inline(always)]
    fn on_stop(&mut self) -> impl std::future::Future<Output = ()> + Send {
        self.stopped.fetch_add(1, Ordering::Release);
        std::future::ready(())
    }
}

struct AfAsyncIoSerialActor {
    completed: Arc<AtomicU64>,
    sleep: Duration,
}

impl AfAsyncActor for AfAsyncIoSerialActor {
    type Msg = ();

    #[inline(always)]
    fn handle_tell(&mut self, _msg: Self::Msg) -> impl std::future::Future<Output = ()> + Send {
        let completed = Arc::clone(&self.completed);
        let sleep = self.sleep;
        async move {
            tokio::time::sleep(sleep).await;
            completed.fetch_add(1, Ordering::Release);
        }
    }
}

struct AfAsyncIoOneShotActor {
    completed: Arc<AtomicU64>,
    sleep: Duration,
}

impl AfAsyncActor for AfAsyncIoOneShotActor {
    type Msg = ();
    const USES_ASYNC_CONTEXT: bool = true;

    #[inline(always)]
    fn handle_tell(&mut self, _msg: Self::Msg) -> impl std::future::Future<Output = ()> + Send {
        std::future::ready(())
    }

    #[inline(always)]
    fn handle_tell_with_ctx(
        &mut self,
        _msg: Self::Msg,
        ctx: &mut AfAsyncContext,
    ) -> impl std::future::Future<Output = ()> + Send {
        let completed = Arc::clone(&self.completed);
        let sleep = self.sleep;
        ctx.request_shutdown();
        async move {
            tokio::time::sleep(sleep).await;
            completed.fetch_add(1, Ordering::Release);
        }
    }
}

#[inline(always)]
fn run_micro_compute(iters: u32) -> u64 {
    let mut x = 0x9E37_79B9_7F4A_7C15u64;
    for i in 0..iters {
        x = x
            .wrapping_mul(6364136223846793005u64)
            .wrapping_add((i as u64).wrapping_add(1442695040888963407u64))
            ^ (x >> 29);
    }
    x
}

#[inline(always)]
fn sum_completed(counters: &[Arc<AtomicU64>]) -> u64 {
    counters
        .iter()
        .map(|counter| counter.load(Ordering::Acquire))
        .sum::<u64>()
}

#[inline(always)]
fn pubsub_topic_idx(seq: u64, topics: usize, hot_topic: bool) -> usize {
    if hot_topic {
        0
    } else {
        (seq as usize) % topics
    }
}

#[inline(always)]
fn expected_pubsub_deliveries(msgs: u64, subs_per_topic: usize) -> u64 {
    msgs.saturating_mul(subs_per_topic as u64)
}

#[inline(always)]
fn expected_broadcast_deliveries(msgs: u64, subs: usize) -> u64 {
    msgs.saturating_mul(subs as u64)
}

struct AfAsyncWorkSerialActor {
    completed: Arc<AtomicU64>,
    iters: u32,
}

impl AfAsyncActor for AfAsyncWorkSerialActor {
    type Msg = ();

    #[inline(always)]
    fn handle_tell(&mut self, _msg: Self::Msg) -> impl std::future::Future<Output = ()> + Send {
        black_box(run_micro_compute(self.iters));
        self.completed.fetch_add(1, Ordering::Release);
        std::future::ready(())
    }
}

struct AfAsyncWorkOneShotActor {
    completed: Arc<AtomicU64>,
    iters: u32,
}

impl AfAsyncActor for AfAsyncWorkOneShotActor {
    type Msg = ();
    const USES_ASYNC_CONTEXT: bool = true;

    #[inline(always)]
    fn handle_tell(&mut self, _msg: Self::Msg) -> impl std::future::Future<Output = ()> + Send {
        std::future::ready(())
    }

    #[inline(always)]
    fn handle_tell_with_ctx(
        &mut self,
        _msg: Self::Msg,
        ctx: &mut AfAsyncContext,
    ) -> impl std::future::Future<Output = ()> + Send {
        ctx.request_shutdown();
        black_box(run_micro_compute(self.iters));
        self.completed.fetch_add(1, Ordering::Release);
        std::future::ready(())
    }
}

struct AfAsyncSpawnActor {
    started_token: Arc<AtomicU64>,
    token: u64,
}

impl AfAsyncActor for AfAsyncSpawnActor {
    type Msg = ();

    #[inline(always)]
    fn on_start(&mut self) -> impl std::future::Future<Output = ()> + Send {
        self.started_token.store(self.token, Ordering::Release);
        std::future::ready(())
    }

    #[inline(always)]
    fn handle_tell(&mut self, _msg: Self::Msg) -> impl std::future::Future<Output = ()> + Send {
        std::future::ready(())
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

struct RactorAsyncSpawnActor;

impl RactorActor for RactorAsyncSpawnActor {
    type Msg = ();
    type State = ();
    type Arguments = (Arc<AtomicU64>, u64);

    #[inline(always)]
    fn pre_start(
        &self,
        _myself: RactorActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> impl std::future::Future<Output = Result<Self::State, RactorActorProcessingErr>> + Send
    {
        let (started_token, token) = args;
        started_token.store(token, Ordering::Release);
        std::future::ready(Ok(()))
    }

    #[inline(always)]
    fn handle(
        &self,
        _myself: RactorActorRef<Self::Msg>,
        _message: Self::Msg,
        _state: &mut Self::State,
    ) -> impl std::future::Future<Output = Result<(), RactorActorProcessingErr>> + Send {
        std::future::ready(Ok(()))
    }
}

struct RactorAsyncWorkActor;

impl RactorActor for RactorAsyncWorkActor {
    type Msg = ();
    type State = (Arc<AtomicU64>, u32);
    type Arguments = (Arc<AtomicU64>, u32);

    #[inline(always)]
    fn pre_start(
        &self,
        _myself: RactorActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> impl std::future::Future<Output = Result<Self::State, RactorActorProcessingErr>> + Send
    {
        std::future::ready(Ok(args))
    }

    #[inline(always)]
    fn handle(
        &self,
        _myself: RactorActorRef<Self::Msg>,
        _message: Self::Msg,
        state: &mut Self::State,
    ) -> impl std::future::Future<Output = Result<(), RactorActorProcessingErr>> + Send {
        let (processed, iters) = state;
        black_box(run_micro_compute(*iters));
        processed.fetch_add(1, Ordering::Release);
        std::future::ready(Ok(()))
    }
}

struct RactorEphemeralComputeActor;

impl RactorActor for RactorEphemeralComputeActor {
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
        myself: RactorActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> impl std::future::Future<Output = Result<(), RactorActorProcessingErr>> + Send {
        state.store(black_box(message), Ordering::Release);
        myself.stop(None);
        std::future::ready(Ok(()))
    }
}

struct ActixTryTellMsg(u64);

impl ActixMessage for ActixTryTellMsg {
    type Result = ();
}

struct ActixAskMsg(u64);

impl ActixMessage for ActixAskMsg {
    type Result = u64;
}

struct ActixSpawnCycleMsg(u64);

impl ActixMessage for ActixSpawnCycleMsg {
    type Result = ();
}

struct ActixEphemeralComputeMsg(u64);

impl ActixMessage for ActixEphemeralComputeMsg {
    type Result = u64;
}

struct ActixWorkMsg;

impl ActixMessage for ActixWorkMsg {
    type Result = ();
}

struct ActixAsyncStopMsg;

impl ActixMessage for ActixAsyncStopMsg {
    type Result = ();
}

struct ActixAsyncFanoutMsg;

impl ActixMessage for ActixAsyncFanoutMsg {
    type Result = ();
}

struct ActixSyncTryTellActor {
    processed: Arc<AtomicU64>,
}

impl ActixActor for ActixSyncTryTellActor {
    type Context = SyncContext<Self>;
}

impl ActixHandler<ActixTryTellMsg> for ActixSyncTryTellActor {
    type Result = ();

    #[inline(always)]
    fn handle(&mut self, msg: ActixTryTellMsg, _ctx: &mut Self::Context) -> Self::Result {
        black_box(msg.0);
        self.processed.fetch_add(1, Ordering::Relaxed);
    }
}

struct ActixSyncAskActor;

impl ActixActor for ActixSyncAskActor {
    type Context = SyncContext<Self>;
}

impl ActixHandler<ActixAskMsg> for ActixSyncAskActor {
    type Result = u64;

    #[inline(always)]
    fn handle(&mut self, msg: ActixAskMsg, _ctx: &mut Self::Context) -> Self::Result {
        msg.0
    }
}

struct ActixSyncWorkActor {
    processed: Arc<AtomicU64>,
    iters: u32,
}

impl ActixActor for ActixSyncWorkActor {
    type Context = SyncContext<Self>;
}

impl ActixHandler<ActixWorkMsg> for ActixSyncWorkActor {
    type Result = ();

    #[inline(always)]
    fn handle(&mut self, _msg: ActixWorkMsg, _ctx: &mut Self::Context) -> Self::Result {
        black_box(run_micro_compute(self.iters));
        self.processed.fetch_add(1, Ordering::Release);
    }
}

struct ActixSyncSpawnActor {
    pending_token: Arc<AtomicU64>,
    started_token: Arc<AtomicU64>,
}

impl ActixActor for ActixSyncSpawnActor {
    type Context = SyncContext<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        let token = self.pending_token.load(Ordering::Acquire);
        self.started_token.store(token, Ordering::Release);
    }
}

impl ActixHandler<ActixSpawnCycleMsg> for ActixSyncSpawnActor {
    type Result = ();

    #[inline(always)]
    fn handle(&mut self, msg: ActixSpawnCycleMsg, ctx: &mut Self::Context) -> Self::Result {
        black_box(msg.0);
        self.pending_token.store(msg.0, Ordering::Release);
        // In SyncContext, stop() restarts a fresh actor instance from the factory.
        ctx.stop();
    }
}

impl ActixHandler<ActixEphemeralComputeMsg> for ActixSyncSpawnActor {
    type Result = u64;

    #[inline(always)]
    fn handle(&mut self, msg: ActixEphemeralComputeMsg, ctx: &mut Self::Context) -> Self::Result {
        self.pending_token.store(msg.0, Ordering::Release);
        ctx.stop();
        black_box(msg.0).wrapping_add(1)
    }
}

struct ActixAsyncTryTellActor {
    processed: Arc<AtomicU64>,
    mailbox_cap: usize,
}

impl ActixActor for ActixAsyncTryTellActor {
    type Context = ActixContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.set_mailbox_capacity(self.mailbox_cap);
    }
}

impl ActixHandler<ActixTryTellMsg> for ActixAsyncTryTellActor {
    type Result = ();

    #[inline(always)]
    fn handle(&mut self, msg: ActixTryTellMsg, _ctx: &mut Self::Context) -> Self::Result {
        black_box(msg.0);
        self.processed.fetch_add(1, Ordering::Relaxed);
    }
}

struct ActixAsyncAskActor {
    mailbox_cap: usize,
}

impl ActixActor for ActixAsyncAskActor {
    type Context = ActixContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.set_mailbox_capacity(self.mailbox_cap);
    }
}

impl ActixHandler<ActixAskMsg> for ActixAsyncAskActor {
    type Result = u64;

    #[inline(always)]
    fn handle(&mut self, msg: ActixAskMsg, _ctx: &mut Self::Context) -> Self::Result {
        msg.0
    }
}

struct ActixAsyncSpawnActor {
    pending_token: Arc<AtomicU64>,
    started_token: Arc<AtomicU64>,
    mailbox_cap: usize,
}

impl ActixActor for ActixAsyncSpawnActor {
    type Context = ActixContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.set_mailbox_capacity(self.mailbox_cap);
        let token = self.pending_token.load(Ordering::Acquire);
        self.started_token.store(token, Ordering::Release);
    }
}

impl ActixHandler<ActixAsyncStopMsg> for ActixAsyncSpawnActor {
    type Result = ();

    #[inline(always)]
    fn handle(&mut self, _msg: ActixAsyncStopMsg, ctx: &mut Self::Context) -> Self::Result {
        ctx.stop();
    }
}

struct ActixAsyncFanoutActor {
    mailbox_cap: usize,
    stopped: Arc<AtomicU64>,
}

impl ActixActor for ActixAsyncFanoutActor {
    type Context = ActixContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.set_mailbox_capacity(self.mailbox_cap);
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        self.stopped.fetch_add(1, Ordering::Release);
    }
}

impl ActixHandler<ActixAsyncFanoutMsg> for ActixAsyncFanoutActor {
    type Result = ();

    #[inline(always)]
    fn handle(&mut self, _msg: ActixAsyncFanoutMsg, ctx: &mut Self::Context) -> Self::Result {
        ctx.stop();
    }
}

struct ActixAsyncWorkActor {
    processed: Arc<AtomicU64>,
    iters: u32,
    mailbox_cap: usize,
}

impl ActixActor for ActixAsyncWorkActor {
    type Context = ActixContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.set_mailbox_capacity(self.mailbox_cap);
    }
}

impl ActixHandler<ActixWorkMsg> for ActixAsyncWorkActor {
    type Result = ();

    #[inline(always)]
    fn handle(&mut self, _msg: ActixWorkMsg, _ctx: &mut Self::Context) -> Self::Result {
        black_box(run_micro_compute(self.iters));
        self.processed.fetch_add(1, Ordering::Release);
    }
}

struct RactorPubSubSubscriber;

impl RactorActor for RactorPubSubSubscriber {
    type Msg = u64;
    type State = Arc<AtomicU64>;
    type Arguments = Arc<AtomicU64>;

    #[inline(always)]
    fn pre_start(
        &self,
        _myself: RactorActorRef<Self::Msg>,
        delivered: Self::Arguments,
    ) -> impl std::future::Future<Output = Result<Self::State, RactorActorProcessingErr>> + Send
    {
        std::future::ready(Ok(delivered))
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

fn run_af_sync_try_tell(cfg: Config, iters: u64) -> Duration {
    let processed = Arc::new(AtomicU64::new(0));
    let (addr, handle) = sync::spawn_runtime_mailbox_with_opts(
        AfSyncTryTellActor {
            processed: Arc::clone(&processed),
        },
        sync::SpawnOpts {
            mailbox_capacity: cfg.sync_mailbox_cap,
            ..sync::SpawnOpts::default()
        },
    );
    handle.wait_for_startup();

    let warmup = cfg.try_tell_ops.min(10_000);
    for _ in 0..warmup {
        let mut spins = 0u32;
        loop {
            match addr.try_tell(sync::SyncTellMessage::Tell(1)) {
                Ok(()) => break,
                Err(_msg) => spin_then_yield(&mut spins),
            }
        }
    }
    while processed.load(Ordering::Relaxed) < warmup {
        let mut spins = 0u32;
        spin_then_yield(&mut spins);
    }
    addr.reset_mailbox_full_events();

    let t0 = Instant::now();
    for _ in 0..iters {
        processed.store(0, Ordering::Relaxed);
        for _ in 0..cfg.try_tell_ops {
            let mut spins = 0u32;
            loop {
                match addr.try_tell(sync::SyncTellMessage::Tell(1)) {
                    Ok(()) => break,
                    Err(_msg) => spin_then_yield(&mut spins),
                }
            }
        }
        while processed.load(Ordering::Relaxed) < cfg.try_tell_ops {
            let mut spins = 0u32;
            spin_then_yield(&mut spins);
        }
    }
    let elapsed = t0.elapsed();
    if std::env::var_os("AF_CMP_DEBUG_FULL").is_some() {
        let full = addr.mailbox_full_events();
        let sent = cfg.try_tell_ops.saturating_mul(iters);
        eprintln!(
            "DEBUG compare_sync_try_tell/actor_framework_sync: mailbox_full_events={} sent={} full_pct={:.4}%",
            full,
            sent,
            if sent > 0 {
                (full as f64) * 100.0 / (sent as f64)
            } else {
                0.0
            }
        );
    }

    handle.shutdown();
    elapsed
}

fn run_actix_sync_try_tell(cfg: Config, iters: u64) -> Duration {
    let ops = cfg.try_tell_ops;
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
            loop {
                match addr.try_send(ActixTryTellMsg(1)) {
                    Ok(()) => break,
                    Err(ActixSendError::Full(_)) => tokio::task::yield_now().await,
                    Err(ActixSendError::Closed(_)) => panic!("actix sync actor closed"),
                }
            }
        }
        while processed.load(Ordering::Relaxed) < warmup {
            tokio::task::yield_now().await;
        }

        let t0 = Instant::now();
        for _ in 0..iters {
            processed.store(0, Ordering::Relaxed);
            for _ in 0..ops {
                loop {
                    match addr.try_send(ActixTryTellMsg(1)) {
                        Ok(()) => break,
                        Err(ActixSendError::Full(_)) => tokio::task::yield_now().await,
                        Err(ActixSendError::Closed(_)) => panic!("actix sync actor closed"),
                    }
                }
            }
            while processed.load(Ordering::Relaxed) < ops {
                tokio::task::yield_now().await;
            }
        }
        t0.elapsed()
    })
}

fn run_af_sync_tell(cfg: Config, iters: u64) -> Duration {
    let processed = Arc::new(AtomicU64::new(0));
    let (addr, handle) = sync::spawn_runtime_mailbox_with_opts(
        AfSyncTryTellActor {
            processed: Arc::clone(&processed),
        },
        sync::SpawnOpts {
            mailbox_capacity: cfg.sync_mailbox_cap,
            ..sync::SpawnOpts::default()
        },
    );
    handle.wait_for_startup();

    let warmup = cfg.try_tell_ops.min(10_000);
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
        for _ in 0..cfg.try_tell_ops {
            let mut spins = 0u32;
            while !addr.tell(sync::SyncTellMessage::Tell(1)) {
                spin_then_yield(&mut spins);
            }
        }
        while processed.load(Ordering::Relaxed) < cfg.try_tell_ops {
            let mut spins = 0u32;
            spin_then_yield(&mut spins);
        }
    }
    let elapsed = t0.elapsed();

    handle.shutdown();
    elapsed
}

fn run_actix_sync_tell(cfg: Config, iters: u64) -> Duration {
    let ops = cfg.try_tell_ops;
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

fn run_af_sync_ask(cfg: Config, iters: u64) -> Duration {
    let (addr, handle) = sync::spawn_runtime_mailbox_with_opts(
        AfSyncAskActor,
        sync::SpawnOpts {
            mailbox_capacity: cfg.sync_mailbox_cap,
            ..sync::SpawnOpts::default()
        },
    );
    handle.wait_for_startup();

    let warmup = cfg.ask_ops.min(2_000);
    for i in 0..warmup {
        let out = addr
            .ask(|reply| sync::SyncTellAskMessage::Ask {
                msg: AfSyncAskMsg::Echo { v: i, reply },
                reply,
            })
            .expect("actor-framework sync ask warmup failed");
        black_box(out);
    }

    let t0 = Instant::now();
    for _ in 0..iters {
        for i in 0..cfg.ask_ops {
            let out = addr
                .ask(|reply| sync::SyncTellAskMessage::Ask {
                    msg: AfSyncAskMsg::Echo { v: i, reply },
                    reply,
                })
                .expect("actor-framework sync ask failed");
            black_box(out);
        }
    }
    let elapsed = t0.elapsed();

    handle.shutdown();
    elapsed
}

fn run_actix_sync_ask(cfg: Config, iters: u64) -> Duration {
    let ops = cfg.ask_ops;
    let threads = cfg.actix_sync_threads;
    let sys = System::new();

    sys.block_on(async move {
        let addr = SyncArbiter::start(threads, || ActixSyncAskActor);

        let warmup = ops.min(2_000);
        for i in 0..warmup {
            let out = addr
                .send(ActixAskMsg(i))
                .await
                .expect("actix sync ask warmup failed");
            black_box(out);
        }

        let t0 = Instant::now();
        for _ in 0..iters {
            for i in 0..ops {
                let out = addr
                    .send(ActixAskMsg(i))
                    .await
                    .expect("actix sync ask failed");
                black_box(out);
            }
        }
        t0.elapsed()
    })
}

fn run_af_sync_work_single_actor(cfg: Config, iters: u64) -> Duration {
    let ops = cfg.sync_work_ops.max(1);
    let cap = cfg.sync_mailbox_cap.max(1);
    let work_iters = u32::try_from(cfg.sync_work_iters)
        .unwrap_or(u32::MAX)
        .max(1);
    let processed = Arc::new(AtomicU64::new(0));
    let (addr, handle) = sync::spawn_actor(
        cap,
        AfSyncWorkActor {
            processed: Arc::clone(&processed),
            iters: work_iters,
        },
    );
    handle.wait_for_startup();

    let warmup = ops.min(10_000);
    for _ in 0..warmup {
        let mut spins = 0u32;
        loop {
            match addr.try_tell(()) {
                Ok(()) => break,
                Err(_msg) => spin_then_yield(&mut spins),
            }
        }
    }
    while processed.load(Ordering::Acquire) < warmup {
        let mut spins = 0u32;
        spin_then_yield(&mut spins);
    }

    let mut measured = Duration::ZERO;
    for _ in 0..iters {
        processed.store(0, Ordering::Release);
        let t0 = Instant::now();
        for _ in 0..ops {
            let mut spins = 0u32;
            loop {
                match addr.try_tell(()) {
                    Ok(()) => break,
                    Err(_msg) => spin_then_yield(&mut spins),
                }
            }
        }
        while processed.load(Ordering::Acquire) < ops {
            let mut spins = 0u32;
            spin_then_yield(&mut spins);
        }
        measured += t0.elapsed();
    }

    handle.shutdown();
    measured
}

fn run_af_sync_work_sharded_pool(cfg: Config, iters: u64) -> Duration {
    let ops = cfg.sync_work_ops.max(1);
    let cap = cfg.sync_mailbox_cap.max(1);
    let work_iters = u32::try_from(cfg.sync_work_iters)
        .unwrap_or(u32::MAX)
        .max(1);
    let max_shards = usize::try_from(ops).unwrap_or(usize::MAX);
    let shards = cfg.sync_work_shards.max(1).min(max_shards);
    let processed = Arc::new(AtomicU64::new(0));

    // compare_work_sharding_4x forces 4 shards. If the global default pool is size=1,
    // spawning 4 pooled actors via sync::spawn_actor would block on pool saturation.
    // Use a dedicated pool sized to this lane's shard count when needed.
    if shards > cfg.af_sync_pool_size {
        let pool = sync::ContextPool::<AfSyncWorkActor>::with_config(sync::PoolConfig {
            size: shards,
            spin_init: cfg.af_sync_pool_spin_init,
        });
        let mut addrs = Vec::with_capacity(shards);
        let mut handles = Vec::with_capacity(shards);
        for _ in 0..shards {
            let (addr, handle) = pool.spawn(
                cap,
                AfSyncWorkActor {
                    processed: Arc::clone(&processed),
                    iters: work_iters,
                },
            );
            addrs.push(addr);
            handles.push(handle);
        }
        for handle in &handles {
            handle.wait_for_startup();
        }

        let warmup = ops.min(10_000);
        let mut rr = 0usize;
        for _ in 0..warmup {
            let mut spins = 0u32;
            loop {
                match addrs[rr].try_tell(()) {
                    Ok(()) => break,
                    Err(_msg) => spin_then_yield(&mut spins),
                }
            }
            rr += 1;
            if rr == shards {
                rr = 0;
            }
        }
        while processed.load(Ordering::Acquire) < warmup {
            let mut spins = 0u32;
            spin_then_yield(&mut spins);
        }

        let mut measured = Duration::ZERO;
        for _ in 0..iters {
            processed.store(0, Ordering::Release);
            rr = 0;
            let t0 = Instant::now();
            for _ in 0..ops {
                let mut spins = 0u32;
                loop {
                    match addrs[rr].try_tell(()) {
                        Ok(()) => break,
                        Err(_msg) => spin_then_yield(&mut spins),
                    }
                }
                rr += 1;
                if rr == shards {
                    rr = 0;
                }
            }
            while processed.load(Ordering::Acquire) < ops {
                let mut spins = 0u32;
                spin_then_yield(&mut spins);
            }
            measured += t0.elapsed();
        }

        for handle in handles {
            handle.shutdown();
        }
        return measured;
    }

    let mut addrs = Vec::with_capacity(shards);
    let mut handles = Vec::with_capacity(shards);
    for _ in 0..shards {
        let (addr, handle) = sync::spawn_actor(
            cap,
            AfSyncWorkActor {
                processed: Arc::clone(&processed),
                iters: work_iters,
            },
        );
        addrs.push(addr);
        handles.push(handle);
    }
    for handle in &handles {
        handle.wait_for_startup();
    }

    let warmup = ops.min(10_000);
    let mut rr = 0usize;
    for _ in 0..warmup {
        let mut spins = 0u32;
        loop {
            match addrs[rr].try_tell(()) {
                Ok(()) => break,
                Err(_msg) => spin_then_yield(&mut spins),
            }
        }
        rr += 1;
        if rr == shards {
            rr = 0;
        }
    }
    while processed.load(Ordering::Acquire) < warmup {
        let mut spins = 0u32;
        spin_then_yield(&mut spins);
    }

    let mut measured = Duration::ZERO;
    for _ in 0..iters {
        processed.store(0, Ordering::Release);
        rr = 0;
        let t0 = Instant::now();
        for _ in 0..ops {
            let mut spins = 0u32;
            loop {
                match addrs[rr].try_tell(()) {
                    Ok(()) => break,
                    Err(_msg) => spin_then_yield(&mut spins),
                }
            }
            rr += 1;
            if rr == shards {
                rr = 0;
            }
        }
        while processed.load(Ordering::Acquire) < ops {
            let mut spins = 0u32;
            spin_then_yield(&mut spins);
        }
        measured += t0.elapsed();
    }

    for handle in handles {
        handle.shutdown();
    }
    measured
}

fn run_af_sync_work_sharded_spawn_cycle(cfg: Config, iters: u64) -> Duration {
    let ops = cfg.sync_work_ops.max(1);
    let cap = cfg.sync_mailbox_cap.max(1);
    let work_iters = u32::try_from(cfg.sync_work_iters)
        .unwrap_or(u32::MAX)
        .max(1);
    let max_shards = usize::try_from(ops).unwrap_or(usize::MAX);
    let shards = cfg.sync_work_shards.max(1).min(max_shards);

    // Warm up one cycle outside timed section.
    {
        let processed = Arc::new(AtomicU64::new(0));
        let mut addrs = Vec::with_capacity(shards);
        let mut handles = Vec::with_capacity(shards);
        for _ in 0..shards {
            let (addr, handle) = sync::spawn_actor(
                cap,
                AfSyncWorkActor {
                    processed: Arc::clone(&processed),
                    iters: work_iters,
                },
            );
            addrs.push(addr);
            handles.push(handle);
        }
        for handle in &handles {
            handle.wait_for_startup();
        }
        let warmup = ops.min(10_000);
        let mut rr = 0usize;
        for _ in 0..warmup {
            let mut spins = 0u32;
            loop {
                match addrs[rr].try_tell(()) {
                    Ok(()) => break,
                    Err(_msg) => spin_then_yield(&mut spins),
                }
            }
            rr += 1;
            if rr == shards {
                rr = 0;
            }
        }
        while processed.load(Ordering::Acquire) < warmup {
            let mut spins = 0u32;
            spin_then_yield(&mut spins);
        }
        for handle in handles {
            handle.shutdown();
        }
    }

    let mut measured = Duration::ZERO;
    for _ in 0..iters {
        let processed = Arc::new(AtomicU64::new(0));
        let mut addrs = Vec::with_capacity(shards);
        let mut handles = Vec::with_capacity(shards);
        let t0 = Instant::now();
        for _ in 0..shards {
            let (addr, handle) = sync::spawn_actor(
                cap,
                AfSyncWorkActor {
                    processed: Arc::clone(&processed),
                    iters: work_iters,
                },
            );
            addrs.push(addr);
            handles.push(handle);
        }
        for handle in &handles {
            handle.wait_for_startup();
        }

        let mut rr = 0usize;
        for _ in 0..ops {
            let mut spins = 0u32;
            loop {
                match addrs[rr].try_tell(()) {
                    Ok(()) => break,
                    Err(_msg) => spin_then_yield(&mut spins),
                }
            }
            rr += 1;
            if rr == shards {
                rr = 0;
            }
        }
        while processed.load(Ordering::Acquire) < ops {
            let mut spins = 0u32;
            spin_then_yield(&mut spins);
        }
        for handle in handles {
            handle.shutdown();
        }
        measured += t0.elapsed();
    }
    measured
}

fn run_actix_sync_work_single_actor(cfg: Config, iters: u64) -> Duration {
    let ops = cfg.sync_work_ops.max(1);
    let work_iters = u32::try_from(cfg.sync_work_iters)
        .unwrap_or(u32::MAX)
        .max(1);
    let sys = System::new();

    sys.block_on(async move {
        let processed = Arc::new(AtomicU64::new(0));
        let processed_for_factory = Arc::clone(&processed);
        let addr = SyncArbiter::start(1, move || ActixSyncWorkActor {
            processed: Arc::clone(&processed_for_factory),
            iters: work_iters,
        });

        let warmup = ops.min(10_000);
        for _ in 0..warmup {
            addr.do_send(ActixWorkMsg);
        }
        while processed.load(Ordering::Acquire) < warmup {
            tokio::task::yield_now().await;
        }

        let mut measured = Duration::ZERO;
        for _ in 0..iters {
            processed.store(0, Ordering::Release);
            let t0 = Instant::now();
            for _ in 0..ops {
                addr.do_send(ActixWorkMsg);
            }
            while processed.load(Ordering::Acquire) < ops {
                tokio::task::yield_now().await;
            }
            measured += t0.elapsed();
        }
        measured
    })
}

fn run_actix_sync_work_sharded_4x(cfg: Config, iters: u64) -> Duration {
    let ops = cfg.sync_work_ops.max(1);
    let work_iters = u32::try_from(cfg.sync_work_iters)
        .unwrap_or(u32::MAX)
        .max(1);
    let shards = 4usize;
    let sys = System::new();

    sys.block_on(async move {
        let processed = Arc::new(AtomicU64::new(0));
        let mut addrs = Vec::with_capacity(shards);
        for _ in 0..shards {
            let processed_for_factory = Arc::clone(&processed);
            let addr = SyncArbiter::start(1, move || ActixSyncWorkActor {
                processed: Arc::clone(&processed_for_factory),
                iters: work_iters,
            });
            addrs.push(addr);
        }

        let warmup = ops.min(10_000);
        let mut rr = 0usize;
        for _ in 0..warmup {
            addrs[rr].do_send(ActixWorkMsg);
            rr += 1;
            if rr == shards {
                rr = 0;
            }
        }
        while processed.load(Ordering::Acquire) < warmup {
            tokio::task::yield_now().await;
        }

        let mut measured = Duration::ZERO;
        for _ in 0..iters {
            processed.store(0, Ordering::Release);
            rr = 0;
            let t0 = Instant::now();
            for _ in 0..ops {
                addrs[rr].do_send(ActixWorkMsg);
                rr += 1;
                if rr == shards {
                    rr = 0;
                }
            }
            while processed.load(Ordering::Acquire) < ops {
                tokio::task::yield_now().await;
            }
            measured += t0.elapsed();
        }
        measured
    })
}

fn run_af_async_pubsub_local(cfg: Config, iters: u64) -> Duration {
    let topics = cfg.pubsub_topics.max(1);
    let subs_per_topic = cfg.pubsub_subs_per_topic.max(1);
    let msgs = cfg.pubsub_msgs.max(1);
    let warmup = cfg.pubsub_warmup;
    let cap = cfg.pubsub_mailbox_cap.max(1);
    let topic_names: Vec<String> = (0..topics).map(|i| format!("af/cmp/pubsub/{i}")).collect();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cfg.pubsub_runtime_threads)
        .enable_time()
        .build()
        .expect("failed to build tokio runtime");

    rt.block_on(async move {
        let pubsub =
            local_async::PubSub::<u64>::with_broadcast_config(local_async::BroadcastConfig {
                stripes: cfg.pubsub_stripes.max(1),
                concurrent_threshold: cfg.pubsub_threshold.max(1),
            });
        let topic_handles: Vec<local_async::AsyncTopicHandle<u64>> = topic_names
            .iter()
            .map(|topic| pubsub.topic_async(topic))
            .collect();
        let delivered = Arc::new(AtomicU64::new(0));
        let mut handles = Vec::with_capacity(topics.saturating_mul(subs_per_topic));

        for topic in &topic_names {
            for _ in 0..subs_per_topic {
                let (addr, handle) = local_async::spawn_actor(
                    cap,
                    AfAsyncTryTellActor {
                        processed: Arc::clone(&delivered),
                    },
                );
                let _ = pubsub.subscribe(topic, addr);
                handles.push(handle);
            }
        }
        let topic_publishers: Vec<local_async::AsyncTopicPublisher<u64>> = topic_handles
            .iter()
            .map(|topic| topic.publisher())
            .collect();

        let warmup_target = expected_pubsub_deliveries(warmup, subs_per_topic);
        for i in 0..warmup {
            let idx = pubsub_topic_idx(i, topics, cfg.pubsub_hot_topic);
            topic_publishers[idx].publish_fast_stable(1);
        }
        while delivered.load(Ordering::Relaxed) < warmup_target {
            tokio::task::yield_now().await;
        }

        let mut measured = Duration::ZERO;
        let target = expected_pubsub_deliveries(msgs, subs_per_topic);
        for _ in 0..iters {
            delivered.store(0, Ordering::Relaxed);
            let t0 = Instant::now();
            for i in 0..msgs {
                let idx = pubsub_topic_idx(i, topics, cfg.pubsub_hot_topic);
                topic_publishers[idx].publish_fast_stable(1);
            }
            while delivered.load(Ordering::Relaxed) < target {
                tokio::task::yield_now().await;
            }
            measured += t0.elapsed();
        }

        for handle in handles {
            handle.shutdown().await;
        }
        measured
    })
}

fn run_actix_async_pubsub_loop_tell(cfg: Config, iters: u64) -> Duration {
    let topics = cfg.pubsub_topics.max(1);
    let subs_per_topic = cfg.pubsub_subs_per_topic.max(1);
    let msgs = cfg.pubsub_msgs.max(1);
    let warmup = cfg.pubsub_warmup;
    let cap = cfg.pubsub_mailbox_cap.max(1);
    let sys = System::new();

    sys.block_on(async move {
        let delivered = Arc::new(AtomicU64::new(0));
        let mut topic_subscribers = Vec::with_capacity(topics);
        for _ in 0..topics {
            let mut subscribers = Vec::with_capacity(subs_per_topic);
            for _ in 0..subs_per_topic {
                subscribers.push(
                    ActixAsyncTryTellActor {
                        processed: Arc::clone(&delivered),
                        mailbox_cap: cap,
                    }
                    .start(),
                );
            }
            topic_subscribers.push(subscribers);
        }

        delivered.store(0, Ordering::Relaxed);
        let warmup_target = expected_pubsub_deliveries(warmup, subs_per_topic);
        for i in 0..warmup {
            let idx = pubsub_topic_idx(i, topics, cfg.pubsub_hot_topic);
            for subscriber in &topic_subscribers[idx] {
                loop {
                    match subscriber.try_send(ActixTryTellMsg(1)) {
                        Ok(()) => break,
                        Err(ActixSendError::Full(_)) => tokio::task::yield_now().await,
                        Err(ActixSendError::Closed(_)) => {
                            panic!("actix async pubsub subscriber closed")
                        }
                    }
                }
            }
        }
        while delivered.load(Ordering::Relaxed) < warmup_target {
            tokio::task::yield_now().await;
        }

        let mut measured = Duration::ZERO;
        let target = expected_pubsub_deliveries(msgs, subs_per_topic);
        for _ in 0..iters {
            delivered.store(0, Ordering::Relaxed);
            let t0 = Instant::now();
            for i in 0..msgs {
                let idx = pubsub_topic_idx(i, topics, cfg.pubsub_hot_topic);
                for subscriber in &topic_subscribers[idx] {
                    loop {
                        match subscriber.try_send(ActixTryTellMsg(1)) {
                            Ok(()) => break,
                            Err(ActixSendError::Full(_)) => tokio::task::yield_now().await,
                            Err(ActixSendError::Closed(_)) => {
                                panic!("actix async pubsub subscriber closed")
                            }
                        }
                    }
                }
            }
            while delivered.load(Ordering::Relaxed) < target {
                tokio::task::yield_now().await;
            }
            measured += t0.elapsed();
        }

        drop(topic_subscribers);
        measured
    })
}

fn run_ractor_async_pubsub_local(cfg: Config, iters: u64) -> Duration {
    let topics = cfg.pubsub_topics.max(1);
    let subs_per_topic = cfg.pubsub_subs_per_topic.max(1);
    let msgs = cfg.pubsub_msgs.max(1);
    let warmup = cfg.pubsub_warmup;
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cfg.pubsub_runtime_threads)
        .enable_time()
        .build()
        .expect("failed to build tokio runtime");

    rt.block_on(async move {
        let mut output_ports = Vec::with_capacity(topics);
        for _ in 0..topics {
            output_ports.push(RactorOutputPort::<u64>::default());
        }

        let delivered = Arc::new(AtomicU64::new(0));
        let mut subscribers = Vec::with_capacity(topics.saturating_mul(subs_per_topic));
        for topic_idx in 0..topics {
            for _ in 0..subs_per_topic {
                let (subscriber, _handle) = RactorPubSubSubscriber::spawn(
                    None,
                    RactorPubSubSubscriber,
                    Arc::clone(&delivered),
                )
                .await
                .expect("failed to spawn ractor pubsub subscriber");
                output_ports[topic_idx].subscribe(subscriber.clone(), Some);
                subscribers.push(subscriber);
            }
        }

        for _ in 0..8 {
            tokio::task::yield_now().await;
        }

        delivered.store(0, Ordering::Relaxed);
        let warmup_target = expected_pubsub_deliveries(warmup, subs_per_topic);
        for i in 0..warmup {
            let idx = pubsub_topic_idx(i, topics, cfg.pubsub_hot_topic);
            output_ports[idx].send(1);
        }
        while delivered.load(Ordering::Relaxed) < warmup_target {
            tokio::task::yield_now().await;
        }

        let mut measured = Duration::ZERO;
        let target = expected_pubsub_deliveries(msgs, subs_per_topic);
        for _ in 0..iters {
            delivered.store(0, Ordering::Relaxed);
            let t0 = Instant::now();
            for i in 0..msgs {
                let idx = pubsub_topic_idx(i, topics, cfg.pubsub_hot_topic);
                output_ports[idx].send(1);
            }
            while delivered.load(Ordering::Relaxed) < target {
                tokio::task::yield_now().await;
            }
            measured += t0.elapsed();
        }

        for subscriber in subscribers {
            subscriber.stop(None);
        }
        measured
    })
}

fn run_af_sync_broadcast_local(cfg: Config, iters: u64) -> Duration {
    let subs = cfg.broadcast_subs.max(1);
    let msgs = cfg.broadcast_msgs.max(1);
    let warmup = cfg.broadcast_warmup;
    let cap = cfg.broadcast_mailbox_cap.max(1);
    let group = sync::BroadcastGroup::<u64>::with_config(sync::BroadcastConfig {
        stripes: cfg.broadcast_stripes.max(1),
        concurrent_threshold: cfg.broadcast_threshold.max(1),
    });
    let delivered = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::with_capacity(subs);
    for _ in 0..subs {
        let delivered2 = Arc::clone(&delivered);
        let (addr, handle) = sync::mpsc::spawn(cap, move |_msg: u64| {
            delivered2.fetch_add(1, Ordering::Relaxed);
        });
        let _ = group.subscribe(addr);
        handles.push(handle);
    }
    let publisher = group.publisher();

    let warmup_target = expected_broadcast_deliveries(warmup, subs);
    for _ in 0..warmup {
        publisher.publish_fast_stable(1);
    }
    let warmup_deadline = Instant::now() + Duration::from_secs(20);
    while delivered.load(Ordering::Acquire) < warmup_target {
        if Instant::now() >= warmup_deadline {
            panic!("actor-framework sync broadcast warmup timed out waiting for delivery");
        }
        std::thread::yield_now();
    }

    let mut measured = Duration::ZERO;
    let target_per_iter = expected_broadcast_deliveries(msgs, subs);
    let mut expected = warmup_target;
    for _ in 0..iters {
        let t0 = Instant::now();
        for _ in 0..msgs {
            publisher.publish_fast_stable(1);
        }
        expected = expected.saturating_add(target_per_iter);
        let iter_deadline = Instant::now() + Duration::from_secs(20);
        while delivered.load(Ordering::Acquire) < expected {
            if Instant::now() >= iter_deadline {
                panic!("actor-framework sync broadcast timed out waiting for delivery");
            }
            std::thread::yield_now();
        }
        measured += t0.elapsed();
    }

    for handle in handles {
        handle.shutdown();
    }
    measured
}

fn run_actix_sync_broadcast_loop_tell(cfg: Config, iters: u64) -> Duration {
    let subs = cfg.broadcast_subs.max(1);
    let msgs = cfg.broadcast_msgs.max(1);
    let warmup = cfg.broadcast_warmup;
    let sys = System::new();

    sys.block_on(async move {
        let delivered = Arc::new(AtomicU64::new(0));
        let mut subscribers = Vec::with_capacity(subs);
        for _ in 0..subs {
            let processed_for_factory = Arc::clone(&delivered);
            let addr = SyncArbiter::start(1, move || ActixSyncTryTellActor {
                processed: Arc::clone(&processed_for_factory),
            });
            subscribers.push(addr);
        }

        let warmup_target = expected_broadcast_deliveries(warmup, subs);
        for _ in 0..warmup {
            for subscriber in &subscribers {
                subscriber.do_send(ActixTryTellMsg(1));
            }
        }
        while delivered.load(Ordering::Acquire) < warmup_target {
            tokio::task::yield_now().await;
        }

        let mut measured = Duration::ZERO;
        let target_per_iter = expected_broadcast_deliveries(msgs, subs);
        let mut expected = warmup_target;
        for _ in 0..iters {
            let t0 = Instant::now();
            for _ in 0..msgs {
                for subscriber in &subscribers {
                    subscriber.do_send(ActixTryTellMsg(1));
                }
            }
            expected = expected.saturating_add(target_per_iter);
            while delivered.load(Ordering::Acquire) < expected {
                tokio::task::yield_now().await;
            }
            measured += t0.elapsed();
        }
        measured
    })
}

fn run_af_sync_broadcast_loop_tell(cfg: Config, iters: u64) -> Duration {
    let subs = cfg.broadcast_subs.max(1);
    let msgs = cfg.broadcast_msgs.max(1);
    let warmup = cfg.broadcast_warmup;
    let cap = cfg.broadcast_mailbox_cap.max(1);
    let delivered = Arc::new(AtomicU64::new(0));
    let mut subscribers = Vec::with_capacity(subs);
    let mut handles = Vec::with_capacity(subs);
    for _ in 0..subs {
        let delivered2 = Arc::clone(&delivered);
        let (addr, handle) = sync::mpsc::spawn(cap, move |_msg: u64| {
            delivered2.fetch_add(1, Ordering::Relaxed);
        });
        subscribers.push(addr);
        handles.push(handle);
    }

    let warmup_target = expected_broadcast_deliveries(warmup, subs);
    for _ in 0..warmup {
        for subscriber in &subscribers {
            let mut spins = 0u32;
            loop {
                match subscriber.try_tell(1) {
                    Ok(()) => break,
                    Err(_msg) => spin_then_yield(&mut spins),
                }
            }
        }
    }
    while delivered.load(Ordering::Acquire) < warmup_target {
        std::thread::yield_now();
    }

    let target_per_iter = expected_broadcast_deliveries(msgs, subs);
    let mut expected = warmup_target;
    let mut measured = Duration::ZERO;
    for _ in 0..iters {
        let t0 = Instant::now();
        for _ in 0..msgs {
            for subscriber in &subscribers {
                let mut spins = 0u32;
                loop {
                    match subscriber.try_tell(1) {
                        Ok(()) => break,
                        Err(_msg) => spin_then_yield(&mut spins),
                    }
                }
            }
        }
        expected = expected.saturating_add(target_per_iter);
        while delivered.load(Ordering::Acquire) < expected {
            std::thread::yield_now();
        }
        measured += t0.elapsed();
    }

    for handle in handles {
        handle.shutdown();
    }
    measured
}

fn run_af_async_broadcast_loop_tell(cfg: Config, iters: u64) -> Duration {
    let subs = cfg.broadcast_subs.max(1);
    let msgs = cfg.broadcast_msgs.max(1);
    let warmup = cfg.broadcast_warmup;
    let cap = cfg.broadcast_mailbox_cap.max(1);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cfg.async_runtime_threads)
        .enable_time()
        .build()
        .expect("failed to build tokio runtime");

    rt.block_on(async move {
        let delivered = Arc::new(AtomicU64::new(0));
        let mut subscribers = Vec::with_capacity(subs);
        let mut handles = Vec::with_capacity(subs);
        for _ in 0..subs {
            let (addr, handle) = local_async::spawn_actor(
                cap,
                AfAsyncTryTellActor {
                    processed: Arc::clone(&delivered),
                },
            );
            subscribers.push(addr);
            handles.push(handle);
        }

        let warmup_target = expected_broadcast_deliveries(warmup, subs);
        for _ in 0..warmup {
            for subscriber in &subscribers {
                loop {
                    match subscriber.try_tell(1) {
                        Ok(()) => break,
                        Err(local_async::TryTellError::Full(_)) => tokio::task::yield_now().await,
                        Err(local_async::TryTellError::Closed(_)) => {
                            panic!("actor-framework async broadcast subscriber closed")
                        }
                    }
                }
            }
        }
        while delivered.load(Ordering::Acquire) < warmup_target {
            tokio::task::yield_now().await;
        }

        let mut measured = Duration::ZERO;
        let target_per_iter = expected_broadcast_deliveries(msgs, subs);
        let mut expected = warmup_target;
        for _ in 0..iters {
            let t0 = Instant::now();
            for _ in 0..msgs {
                for subscriber in &subscribers {
                    loop {
                        match subscriber.try_tell(1) {
                            Ok(()) => break,
                            Err(local_async::TryTellError::Full(_)) => {
                                tokio::task::yield_now().await
                            }
                            Err(local_async::TryTellError::Closed(_)) => {
                                panic!("actor-framework async broadcast subscriber closed")
                            }
                        }
                    }
                }
            }
            expected = expected.saturating_add(target_per_iter);
            while delivered.load(Ordering::Acquire) < expected {
                tokio::task::yield_now().await;
            }
            measured += t0.elapsed();
        }

        for handle in handles {
            handle.shutdown().await;
        }
        measured
    })
}

fn run_af_async_broadcast_local(cfg: Config, iters: u64) -> Duration {
    let subs = cfg.broadcast_subs.max(1);
    let msgs = cfg.broadcast_msgs.max(1);
    let warmup = cfg.broadcast_warmup;
    let cap = cfg.broadcast_mailbox_cap.max(1);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cfg.async_runtime_threads)
        .enable_time()
        .build()
        .expect("failed to build tokio runtime");

    rt.block_on(async move {
        let broadcast =
            local_async::BroadcastGroup::<u64>::with_config(local_async::BroadcastConfig {
                stripes: cfg.broadcast_stripes.max(1),
                concurrent_threshold: cfg.broadcast_threshold.max(1),
            });

        let delivered = Arc::new(AtomicU64::new(0));
        let mut handles = Vec::with_capacity(subs);
        for _ in 0..subs {
            let (addr, handle) = local_async::spawn_actor(
                cap,
                AfAsyncTryTellActor {
                    processed: Arc::clone(&delivered),
                },
            );
            let _ = broadcast.subscribe(addr);
            handles.push(handle);
        }
        let publisher = broadcast.publisher();

        let warmup_target = expected_broadcast_deliveries(warmup, subs);
        for _ in 0..warmup {
            publisher.publish_fast_stable(1);
        }
        let warmup_deadline = Instant::now() + Duration::from_secs(20);
        while delivered.load(Ordering::Acquire) < warmup_target {
            if Instant::now() >= warmup_deadline {
                panic!("actor-framework async broadcast warmup timed out waiting for delivery");
            }
            tokio::task::yield_now().await;
        }

        let mut measured = Duration::ZERO;
        let target_per_iter = expected_broadcast_deliveries(msgs, subs);
        let mut expected = warmup_target;
        for _ in 0..iters {
            let t0 = Instant::now();
            for _ in 0..msgs {
                publisher.publish_fast_stable(1);
            }
            expected = expected.saturating_add(target_per_iter);
            let iter_deadline = Instant::now() + Duration::from_secs(20);
            while delivered.load(Ordering::Acquire) < expected {
                if Instant::now() >= iter_deadline {
                    panic!("actor-framework async broadcast timed out waiting for delivery");
                }
                tokio::task::yield_now().await;
            }
            measured += t0.elapsed();
        }

        for handle in handles {
            handle.shutdown().await;
        }
        measured
    })
}

fn run_actix_async_broadcast_loop_tell(cfg: Config, iters: u64) -> Duration {
    let subs = cfg.broadcast_subs.max(1);
    let msgs = cfg.broadcast_msgs.max(1);
    let warmup = cfg.broadcast_warmup;
    let cap = cfg.broadcast_mailbox_cap.max(1);
    let sys = System::new();

    sys.block_on(async move {
        let delivered = Arc::new(AtomicU64::new(0));
        let mut subscribers = Vec::with_capacity(subs);
        for _ in 0..subs {
            subscribers.push(
                ActixAsyncTryTellActor {
                    processed: Arc::clone(&delivered),
                    mailbox_cap: cap,
                }
                .start(),
            );
        }

        let warmup_target = expected_broadcast_deliveries(warmup, subs);
        for _ in 0..warmup {
            for subscriber in &subscribers {
                loop {
                    match subscriber.try_send(ActixTryTellMsg(1)) {
                        Ok(()) => break,
                        Err(ActixSendError::Full(_)) => tokio::task::yield_now().await,
                        Err(ActixSendError::Closed(_)) => {
                            panic!("actix async broadcast subscriber closed")
                        }
                    }
                }
            }
        }
        while delivered.load(Ordering::Acquire) < warmup_target {
            tokio::task::yield_now().await;
        }

        let mut measured = Duration::ZERO;
        let target_per_iter = expected_broadcast_deliveries(msgs, subs);
        let mut expected = warmup_target;
        for _ in 0..iters {
            let t0 = Instant::now();
            for _ in 0..msgs {
                for subscriber in &subscribers {
                    loop {
                        match subscriber.try_send(ActixTryTellMsg(1)) {
                            Ok(()) => break,
                            Err(ActixSendError::Full(_)) => tokio::task::yield_now().await,
                            Err(ActixSendError::Closed(_)) => {
                                panic!("actix async broadcast subscriber closed")
                            }
                        }
                    }
                }
            }
            expected = expected.saturating_add(target_per_iter);
            while delivered.load(Ordering::Acquire) < expected {
                tokio::task::yield_now().await;
            }
            measured += t0.elapsed();
        }

        drop(subscribers);
        measured
    })
}

fn run_ractor_async_broadcast_local(cfg: Config, iters: u64) -> Duration {
    let subs = cfg.broadcast_subs.max(1);
    let msgs = cfg.broadcast_msgs.max(1);
    let warmup = cfg.broadcast_warmup;
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cfg.async_runtime_threads)
        .enable_time()
        .build()
        .expect("failed to build tokio runtime");

    rt.block_on(async move {
        let output_port = RactorOutputPort::<u64>::default();
        let delivered = Arc::new(AtomicU64::new(0));
        let mut subscribers = Vec::with_capacity(subs);
        for _ in 0..subs {
            let (subscriber, _handle) =
                RactorPubSubSubscriber::spawn(None, RactorPubSubSubscriber, Arc::clone(&delivered))
                    .await
                    .expect("failed to spawn ractor broadcast subscriber");
            output_port.subscribe(subscriber.clone(), Some);
            subscribers.push(subscriber);
        }

        for _ in 0..8 {
            tokio::task::yield_now().await;
        }

        let warmup_target = expected_broadcast_deliveries(warmup, subs);
        for _ in 0..warmup {
            output_port.send(1);
        }
        while delivered.load(Ordering::Acquire) < warmup_target {
            tokio::task::yield_now().await;
        }

        let mut measured = Duration::ZERO;
        let target_per_iter = expected_broadcast_deliveries(msgs, subs);
        let mut expected = warmup_target;
        for _ in 0..iters {
            let t0 = Instant::now();
            for _ in 0..msgs {
                output_port.send(1);
            }
            expected = expected.saturating_add(target_per_iter);
            while delivered.load(Ordering::Acquire) < expected {
                tokio::task::yield_now().await;
            }
            measured += t0.elapsed();
        }

        for subscriber in subscribers {
            subscriber.stop(None);
        }
        measured
    })
}

fn run_actix_async_work_single_actor_mt4(cfg: Config, iters: u64) -> Duration {
    let ops = cfg.sync_work_ops.max(1);
    let cap = cfg.async_work_mailbox_cap.max(1);
    let work_iters = u32::try_from(cfg.sync_work_iters)
        .unwrap_or(u32::MAX)
        .max(1);
    let sys = System::new();

    sys.block_on(async move {
        let processed = Arc::new(AtomicU64::new(0));
        let addr = ActixAsyncWorkActor {
            processed: Arc::clone(&processed),
            iters: work_iters,
            mailbox_cap: cap,
        }
        .start();

        let warmup = ops.min(10_000);
        for _ in 0..warmup {
            loop {
                match addr.try_send(ActixWorkMsg) {
                    Ok(()) => break,
                    Err(ActixSendError::Full(_)) => tokio::task::yield_now().await,
                    Err(ActixSendError::Closed(_)) => {
                        panic!("actix async work actor closed during warmup")
                    }
                }
            }
        }
        while processed.load(Ordering::Acquire) < warmup {
            tokio::task::yield_now().await;
        }

        let mut measured = Duration::ZERO;
        for _ in 0..iters {
            processed.store(0, Ordering::Release);
            let t0 = Instant::now();
            for _ in 0..ops {
                loop {
                    match addr.try_send(ActixWorkMsg) {
                        Ok(()) => break,
                        Err(ActixSendError::Full(_)) => tokio::task::yield_now().await,
                        Err(ActixSendError::Closed(_)) => panic!("actix async work actor closed"),
                    }
                }
            }
            while processed.load(Ordering::Acquire) < ops {
                tokio::task::yield_now().await;
            }
            measured += t0.elapsed();
        }
        measured
    })
}

fn run_actix_async_work_sharded_4x_mt4(cfg: Config, iters: u64) -> Duration {
    let ops = cfg.sync_work_ops.max(1);
    let cap = cfg.async_work_mailbox_cap.max(1);
    let work_iters = u32::try_from(cfg.sync_work_iters)
        .unwrap_or(u32::MAX)
        .max(1);
    let shards = 4usize;
    let sys = System::new();

    sys.block_on(async move {
        let mut processed = Vec::with_capacity(shards);
        let mut addrs = Vec::with_capacity(shards);
        for _ in 0..shards {
            let shard_processed = Arc::new(AtomicU64::new(0));
            let addr = ActixAsyncWorkActor {
                processed: Arc::clone(&shard_processed),
                iters: work_iters,
                mailbox_cap: cap,
            }
            .start();
            processed.push(shard_processed);
            addrs.push(addr);
        }

        let warmup = ops.min(10_000);
        let mut rr = 0usize;
        for _ in 0..warmup {
            loop {
                match addrs[rr].try_send(ActixWorkMsg) {
                    Ok(()) => break,
                    Err(ActixSendError::Full(_)) => tokio::task::yield_now().await,
                    Err(ActixSendError::Closed(_)) => {
                        panic!("actix async sharded work actor closed during warmup")
                    }
                }
            }
            rr += 1;
            if rr == shards {
                rr = 0;
            }
        }
        while sum_completed(&processed) < warmup {
            tokio::task::yield_now().await;
        }

        let mut measured = Duration::ZERO;
        for _ in 0..iters {
            for counter in &processed {
                counter.store(0, Ordering::Release);
            }
            rr = 0;
            let t0 = Instant::now();
            for _ in 0..ops {
                loop {
                    match addrs[rr].try_send(ActixWorkMsg) {
                        Ok(()) => break,
                        Err(ActixSendError::Full(_)) => tokio::task::yield_now().await,
                        Err(ActixSendError::Closed(_)) => {
                            panic!("actix async sharded work actor closed")
                        }
                    }
                }
                rr += 1;
                if rr == shards {
                    rr = 0;
                }
            }
            while sum_completed(&processed) < ops {
                tokio::task::yield_now().await;
            }
            measured += t0.elapsed();
        }
        measured
    })
}

fn run_ractor_async_work_single_actor(cfg: Config, iters: u64) -> Duration {
    let ops = cfg.sync_work_ops.max(1);
    let work_iters = u32::try_from(cfg.sync_work_iters)
        .unwrap_or(u32::MAX)
        .max(1);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cfg.async_runtime_threads)
        .enable_time()
        .build()
        .expect("failed to build tokio runtime");

    rt.block_on(async move {
        let processed = Arc::new(AtomicU64::new(0));
        let (addr, handle) = RactorAsyncWorkActor::spawn(
            None,
            RactorAsyncWorkActor,
            (Arc::clone(&processed), work_iters),
        )
        .await
        .expect("failed to spawn ractor async work actor");

        let warmup = ops.min(10_000);
        for _ in 0..warmup {
            addr.send_message(())
                .expect("ractor async work warmup send failed");
        }
        while processed.load(Ordering::Acquire) < warmup {
            tokio::task::yield_now().await;
        }

        let mut measured = Duration::ZERO;
        for _ in 0..iters {
            processed.store(0, Ordering::Release);
            let t0 = Instant::now();
            for _ in 0..ops {
                addr.send_message(())
                    .expect("ractor async work send failed");
            }
            while processed.load(Ordering::Acquire) < ops {
                tokio::task::yield_now().await;
            }
            measured += t0.elapsed();
        }
        addr.stop(None);
        handle.await.expect("ractor async work actor join failed");
        measured
    })
}

fn run_ractor_async_work_sharded_4x(cfg: Config, iters: u64) -> Duration {
    let ops = cfg.sync_work_ops.max(1);
    let work_iters = u32::try_from(cfg.sync_work_iters)
        .unwrap_or(u32::MAX)
        .max(1);
    let shards = 4usize;
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cfg.async_runtime_threads)
        .enable_time()
        .build()
        .expect("failed to build tokio runtime");

    rt.block_on(async move {
        let mut processed = Vec::with_capacity(shards);
        let mut addrs = Vec::with_capacity(shards);
        let mut handles = Vec::with_capacity(shards);
        for _ in 0..shards {
            let shard_processed = Arc::new(AtomicU64::new(0));
            let (addr, handle) = RactorAsyncWorkActor::spawn(
                None,
                RactorAsyncWorkActor,
                (Arc::clone(&shard_processed), work_iters),
            )
            .await
            .expect("failed to spawn ractor async sharded work actor");
            processed.push(shard_processed);
            addrs.push(addr);
            handles.push(handle);
        }

        let warmup = ops.min(10_000);
        let mut rr = 0usize;
        for _ in 0..warmup {
            addrs[rr]
                .send_message(())
                .expect("ractor async sharded warmup send failed");
            rr += 1;
            if rr == shards {
                rr = 0;
            }
        }
        while sum_completed(&processed) < warmup {
            tokio::task::yield_now().await;
        }

        let mut measured = Duration::ZERO;
        for _ in 0..iters {
            for counter in &processed {
                counter.store(0, Ordering::Release);
            }
            rr = 0;
            let t0 = Instant::now();
            for _ in 0..ops {
                addrs[rr]
                    .send_message(())
                    .expect("ractor async sharded send failed");
                rr += 1;
                if rr == shards {
                    rr = 0;
                }
            }
            while sum_completed(&processed) < ops {
                tokio::task::yield_now().await;
            }
            measured += t0.elapsed();
        }

        for addr in addrs {
            addr.stop(None);
        }
        for handle in handles {
            handle
                .await
                .expect("ractor async sharded work actor join failed");
        }
        measured
    })
}

fn run_af_async_work_single_actor_mt4(cfg: Config, iters: u64) -> Duration {
    let ops = cfg.sync_work_ops.max(1);
    let cap = cfg.async_work_mailbox_cap.max(1);
    let work_iters = u32::try_from(cfg.sync_work_iters)
        .unwrap_or(u32::MAX)
        .max(1);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cfg.async_runtime_threads)
        .enable_time()
        .build()
        .expect("failed to build tokio runtime");

    rt.block_on(async move {
        let completed = Arc::new(AtomicU64::new(0));
        let (addr, handle) = local_async::spawn_actor(
            cap,
            AfAsyncWorkSerialActor {
                completed: Arc::clone(&completed),
                iters: work_iters,
            },
        );

        let warmup = ops.min(10_000);
        for _ in 0..warmup {
            loop {
                match addr.try_tell(()) {
                    Ok(()) => break,
                    Err(local_async::TryTellError::Full(())) => tokio::task::yield_now().await,
                    Err(local_async::TryTellError::Closed(())) => {
                        panic!("actor-framework async work single actor closed")
                    }
                }
            }
        }
        while completed.load(Ordering::Acquire) < warmup {
            tokio::task::yield_now().await;
        }

        let mut measured = Duration::ZERO;
        for _ in 0..iters {
            completed.store(0, Ordering::Release);
            let t0 = Instant::now();
            for _ in 0..ops {
                loop {
                    match addr.try_tell(()) {
                        Ok(()) => break,
                        Err(local_async::TryTellError::Full(())) => tokio::task::yield_now().await,
                        Err(local_async::TryTellError::Closed(())) => {
                            panic!("actor-framework async work single actor closed")
                        }
                    }
                }
            }
            while completed.load(Ordering::Acquire) < ops {
                tokio::task::yield_now().await;
            }
            measured += t0.elapsed();
        }

        handle.shutdown().await;
        measured
    })
}

fn run_af_async_work_sharded_4x_mt4(cfg: Config, iters: u64) -> Duration {
    let ops = cfg.sync_work_ops.max(1);
    let cap = cfg.async_work_mailbox_cap.max(1);
    let work_iters = u32::try_from(cfg.sync_work_iters)
        .unwrap_or(u32::MAX)
        .max(1);
    let shards = 4usize;
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cfg.async_runtime_threads)
        .enable_time()
        .build()
        .expect("failed to build tokio runtime");

    rt.block_on(async move {
        let mut completed = Vec::with_capacity(shards);
        let mut addrs = Vec::with_capacity(shards);
        let mut handles = Vec::with_capacity(shards);
        for _ in 0..shards {
            let shard_completed = Arc::new(AtomicU64::new(0));
            let (addr, handle) = local_async::spawn_actor(
                cap,
                AfAsyncWorkSerialActor {
                    completed: Arc::clone(&shard_completed),
                    iters: work_iters,
                },
            );
            completed.push(shard_completed);
            addrs.push(addr);
            handles.push(handle);
        }

        let warmup = ops.min(10_000);
        let mut rr = 0usize;
        for _ in 0..warmup {
            loop {
                match addrs[rr].try_tell(()) {
                    Ok(()) => break,
                    Err(local_async::TryTellError::Full(())) => tokio::task::yield_now().await,
                    Err(local_async::TryTellError::Closed(())) => {
                        panic!("actor-framework async work sharded actor closed")
                    }
                }
            }
            rr += 1;
            if rr == shards {
                rr = 0;
            }
        }
        while sum_completed(&completed) < warmup {
            tokio::task::yield_now().await;
        }

        let mut measured = Duration::ZERO;
        for _ in 0..iters {
            for counter in &completed {
                counter.store(0, Ordering::Release);
            }
            rr = 0;
            let t0 = Instant::now();
            for _ in 0..ops {
                loop {
                    match addrs[rr].try_tell(()) {
                        Ok(()) => break,
                        Err(local_async::TryTellError::Full(())) => tokio::task::yield_now().await,
                        Err(local_async::TryTellError::Closed(())) => {
                            panic!("actor-framework async work sharded actor closed")
                        }
                    }
                }
                rr += 1;
                if rr == shards {
                    rr = 0;
                }
            }
            while sum_completed(&completed) < ops {
                tokio::task::yield_now().await;
            }
            measured += t0.elapsed();
        }

        for handle in handles {
            handle.shutdown().await;
        }
        measured
    })
}

fn run_af_sync_spawn_pool(cfg: Config, iters: u64) -> Duration {
    let ops = cfg.spawn_ops;
    let cap = cfg.spawn_mailbox_cap;
    let started = Arc::new(AtomicU64::new(0));
    let mut token = 0u64;

    // Warm up pooled context checkout/init outside timed section.
    let warmup = ops.min(2_000);
    for _ in 0..warmup {
        token = token.wrapping_add(1);
        started.store(0, Ordering::Relaxed);
        let (_addr, handle) = sync::spawn_actor(
            cap,
            AfSyncSpawnActor {
                started_token: Arc::clone(&started),
                token,
            },
        );
        handle.wait_for_startup();
        while started.load(Ordering::Acquire) != token {
            std::hint::spin_loop();
        }
        handle.shutdown();
    }

    // Measure pooled spawn cycle (spawn-to-ready + stop/return-to-pool).
    let mut measured = Duration::ZERO;
    for _ in 0..iters {
        for _ in 0..ops {
            token = token.wrapping_add(1);
            started.store(0, Ordering::Relaxed);
            let t0 = Instant::now();
            let (_addr, handle) = sync::spawn_actor(
                cap,
                AfSyncSpawnActor {
                    started_token: Arc::clone(&started),
                    token,
                },
            );
            handle.wait_for_startup();
            while started.load(Ordering::Acquire) != token {
                std::hint::spin_loop();
            }
            handle.shutdown();
            measured += t0.elapsed();
        }
    }
    measured
}

fn run_af_sync_respawn_pool(cfg: Config, iters: u64) -> Duration {
    let ops = cfg.spawn_ops;
    let cap = cfg.spawn_mailbox_cap;
    let pending_token = Arc::new(AtomicU64::new(0));
    let started_token = Arc::new(AtomicU64::new(u64::MAX));
    let pending_for_make = Arc::clone(&pending_token);
    let started_for_make = Arc::clone(&started_token);

    let pool = sync::default_pool::<AfSyncSpawnActor>();
    let (_addr, handle) = pool.spawn_reusable(cap, move || AfSyncSpawnActor {
        started_token: Arc::clone(&started_for_make),
        token: pending_for_make.load(Ordering::Acquire),
    });
    if !handle.wait_for_startup() {
        panic!("actor-framework reusable spawn failed before startup");
    }

    while started_token.load(Ordering::Acquire) == u64::MAX {
        std::hint::spin_loop();
    }

    // Warm up restart path outside timed section.
    let mut token = 0u64;
    let warmup = ops.min(2_000);
    for _ in 0..warmup {
        token = token.wrapping_add(1);
        pending_token.store(token, Ordering::Release);
        if !handle.restart_and_wait() {
            panic!("actor-framework reusable restart failed during warmup");
        }
        while started_token.load(Ordering::Acquire) != token {
            std::hint::spin_loop();
        }
    }

    // Measure restart-to-ready (reused mailbox/control on a leased context).
    let mut measured = Duration::ZERO;
    for _ in 0..iters {
        for _ in 0..ops {
            token = token.wrapping_add(1);
            pending_token.store(token, Ordering::Release);
            let t0 = Instant::now();
            if !handle.restart_and_wait() {
                panic!("actor-framework reusable restart failed");
            }
            while started_token.load(Ordering::Acquire) != token {
                std::hint::spin_loop();
            }
            measured += t0.elapsed();
        }
    }
    handle.shutdown();
    measured
}

fn run_actix_sync_spawn_pool(cfg: Config, iters: u64) -> Duration {
    let ops = cfg.spawn_ops;
    let threads = cfg.actix_sync_threads;
    let sys = System::new();

    sys.block_on(async move {
        let pending_token = Arc::new(AtomicU64::new(0));
        let started_token = Arc::new(AtomicU64::new(u64::MAX));
        let pending_for_factory = Arc::clone(&pending_token);
        let started_for_factory = Arc::clone(&started_token);

        let addr = SyncArbiter::start(threads, move || ActixSyncSpawnActor {
            pending_token: Arc::clone(&pending_for_factory),
            started_token: Arc::clone(&started_for_factory),
        });

        while started_token.load(Ordering::Acquire) == u64::MAX {
            tokio::task::yield_now().await;
        }

        // Warm up restart path outside timed section.
        let mut token = 0u64;
        let warmup = ops.min(2_000);
        for _ in 0..warmup {
            token = token.wrapping_add(1);
            addr.do_send(ActixSpawnCycleMsg(token));
            while started_token.load(Ordering::Acquire) != token {
                tokio::task::yield_now().await;
            }
        }

        // Measure restart-to-ready (pooled respawn) only.
        let mut measured = Duration::ZERO;
        for _ in 0..iters {
            for _ in 0..ops {
                token = token.wrapping_add(1);
                let t0 = Instant::now();
                addr.do_send(ActixSpawnCycleMsg(token));
                while started_token.load(Ordering::Acquire) != token {
                    tokio::task::yield_now().await;
                }
                measured += t0.elapsed();
            }
        }
        drop(addr);
        measured
    })
}

fn run_af_async_spawn(cfg: Config, iters: u64) -> Duration {
    let ops = cfg.spawn_ops;
    let cap = cfg.spawn_mailbox_cap;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("failed to build tokio runtime");

    rt.block_on(async move {
        let started_token = Arc::new(AtomicU64::new(0));
        let mut token = 0u64;

        let warmup = ops.min(2_000);
        for _ in 0..warmup {
            token = token.wrapping_add(1);
            started_token.store(0, Ordering::Relaxed);
            let (_addr, handle) = local_async::spawn_actor(
                cap,
                AfAsyncSpawnActor {
                    started_token: Arc::clone(&started_token),
                    token,
                },
            );
            if !handle.wait_for_startup(Duration::from_secs(1)).await {
                panic!("actor-framework async spawn warmup startup timeout");
            }
            while started_token.load(Ordering::Acquire) != token {
                tokio::task::yield_now().await;
            }
            handle.shutdown().await;
        }

        let mut measured = Duration::ZERO;
        for _ in 0..iters {
            for _ in 0..ops {
                token = token.wrapping_add(1);
                started_token.store(0, Ordering::Relaxed);
                let t0 = Instant::now();
                let (_addr, handle) = local_async::spawn_actor(
                    cap,
                    AfAsyncSpawnActor {
                        started_token: Arc::clone(&started_token),
                        token,
                    },
                );
                if !handle.wait_for_startup(Duration::from_secs(1)).await {
                    panic!("actor-framework async spawn startup timeout");
                }
                while started_token.load(Ordering::Acquire) != token {
                    tokio::task::yield_now().await;
                }
                handle.shutdown().await;
                measured += t0.elapsed();
            }
        }
        measured
    })
}

fn run_actix_async_spawn(cfg: Config, iters: u64) -> Duration {
    let ops = cfg.spawn_ops;
    let mailbox_cap = cfg.spawn_mailbox_cap;
    let sys = System::new();

    sys.block_on(async move {
        let pending_token = Arc::new(AtomicU64::new(0));
        let started_token = Arc::new(AtomicU64::new(0));
        let mut token = 0u64;

        let warmup = ops.min(2_000);
        for _ in 0..warmup {
            token = token.wrapping_add(1);
            pending_token.store(token, Ordering::Release);
            started_token.store(0, Ordering::Relaxed);
            let addr = ActixAsyncSpawnActor {
                pending_token: Arc::clone(&pending_token),
                started_token: Arc::clone(&started_token),
                mailbox_cap,
            }
            .start();
            while started_token.load(Ordering::Acquire) != token {
                tokio::task::yield_now().await;
            }
            addr.send(ActixAsyncStopMsg)
                .await
                .expect("actix async spawn warmup stop failed");
        }

        let mut measured = Duration::ZERO;
        for _ in 0..iters {
            for _ in 0..ops {
                token = token.wrapping_add(1);
                pending_token.store(token, Ordering::Release);
                started_token.store(0, Ordering::Relaxed);
                let t0 = Instant::now();
                let addr = ActixAsyncSpawnActor {
                    pending_token: Arc::clone(&pending_token),
                    started_token: Arc::clone(&started_token),
                    mailbox_cap,
                }
                .start();
                while started_token.load(Ordering::Acquire) != token {
                    tokio::task::yield_now().await;
                }
                addr.send(ActixAsyncStopMsg)
                    .await
                    .expect("actix async spawn stop failed");
                measured += t0.elapsed();
            }
        }
        measured
    })
}

fn run_ractor_async_spawn(cfg: Config, iters: u64) -> Duration {
    let ops = cfg.spawn_ops;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("failed to build tokio runtime");

    rt.block_on(async move {
        let started_token = Arc::new(AtomicU64::new(0));
        let mut token = 0u64;

        let warmup = ops.min(2_000);
        for _ in 0..warmup {
            token = token.wrapping_add(1);
            started_token.store(0, Ordering::Relaxed);
            let (addr, handle) = RactorAsyncSpawnActor::spawn(
                None,
                RactorAsyncSpawnActor,
                (Arc::clone(&started_token), token),
            )
            .await
            .expect("failed to spawn ractor async actor (warmup)");
            while started_token.load(Ordering::Acquire) != token {
                tokio::task::yield_now().await;
            }
            addr.stop(None);
            handle.await.expect("ractor async spawn warmup join failed");
        }

        let mut measured = Duration::ZERO;
        for _ in 0..iters {
            for _ in 0..ops {
                token = token.wrapping_add(1);
                started_token.store(0, Ordering::Relaxed);
                let t0 = Instant::now();
                let (addr, handle) = RactorAsyncSpawnActor::spawn(
                    None,
                    RactorAsyncSpawnActor,
                    (Arc::clone(&started_token), token),
                )
                .await
                .expect("failed to spawn ractor async actor");
                while started_token.load(Ordering::Acquire) != token {
                    tokio::task::yield_now().await;
                }
                addr.stop(None);
                handle.await.expect("ractor async spawn join failed");
                measured += t0.elapsed();
            }
        }
        measured
    })
}

fn run_af_async_fanout_shutdown(cfg: Config, iters: u64) -> Duration {
    let actors = usize::try_from(cfg.async_fanout_actors).unwrap_or(usize::MAX / 2);
    let actors = actors.max(1);
    let actors_u64 = u64::try_from(actors).unwrap_or(u64::MAX);
    let cap = cfg.spawn_mailbox_cap;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("failed to build tokio runtime");

    rt.block_on(async move {
        let warmup_cycles = 2u64;
        for _ in 0..warmup_cycles {
            let stopped = Arc::new(AtomicU64::new(0));
            let mut addrs = Vec::with_capacity(actors);
            let mut handles = Vec::with_capacity(actors);
            for _ in 0..actors {
                let (addr, handle) = local_async::spawn_actor(
                    cap,
                    AfAsyncFanoutActor {
                        stopped: Arc::clone(&stopped),
                    },
                );
                addrs.push(addr);
                handles.push(handle);
            }
            for addr in &addrs {
                loop {
                    match addr.try_tell(AfAsyncFanoutMsg::Close) {
                        Ok(()) => break,
                        Err(local_async::TryTellError::Full(AfAsyncFanoutMsg::Close)) => {
                            tokio::task::yield_now().await
                        }
                        Err(local_async::TryTellError::Closed(_)) => {
                            panic!("actor-framework async fanout warmup actor closed")
                        }
                    }
                }
            }
            drop(addrs);
            for handle in handles {
                handle.wait().await;
            }
            while stopped.load(Ordering::Acquire) < actors_u64 {
                tokio::task::yield_now().await;
            }
        }

        let mut measured = Duration::ZERO;
        for _ in 0..iters {
            let t0 = Instant::now();
            let stopped = Arc::new(AtomicU64::new(0));
            let mut addrs = Vec::with_capacity(actors);
            let mut handles = Vec::with_capacity(actors);
            for _ in 0..actors {
                let (addr, handle) = local_async::spawn_actor(
                    cap,
                    AfAsyncFanoutActor {
                        stopped: Arc::clone(&stopped),
                    },
                );
                addrs.push(addr);
                handles.push(handle);
            }
            for addr in &addrs {
                loop {
                    match addr.try_tell(AfAsyncFanoutMsg::Close) {
                        Ok(()) => break,
                        Err(local_async::TryTellError::Full(AfAsyncFanoutMsg::Close)) => {
                            tokio::task::yield_now().await
                        }
                        Err(local_async::TryTellError::Closed(_)) => {
                            panic!("actor-framework async fanout actor closed")
                        }
                    }
                }
            }
            drop(addrs);
            for handle in handles {
                handle.wait().await;
            }
            while stopped.load(Ordering::Acquire) < actors_u64 {
                tokio::task::yield_now().await;
            }
            measured += t0.elapsed();
        }
        measured
    })
}

fn run_actix_async_fanout_shutdown(cfg: Config, iters: u64) -> Duration {
    let actors = usize::try_from(cfg.async_fanout_actors).unwrap_or(usize::MAX / 2);
    let actors = actors.max(1);
    let actors_u64 = u64::try_from(actors).unwrap_or(u64::MAX);
    let cap = cfg.spawn_mailbox_cap;
    let sys = System::new();

    sys.block_on(async move {
        let warmup_cycles = 2u64;
        for _ in 0..warmup_cycles {
            let stopped = Arc::new(AtomicU64::new(0));
            let mut addrs = Vec::with_capacity(actors);
            for _ in 0..actors {
                let addr = ActixAsyncFanoutActor {
                    mailbox_cap: cap,
                    stopped: Arc::clone(&stopped),
                }
                .start();
                addrs.push(addr);
            }
            for addr in addrs {
                addr.do_send(ActixAsyncFanoutMsg);
            }
            while stopped.load(Ordering::Acquire) < actors_u64 {
                tokio::task::yield_now().await;
            }
        }

        let mut measured = Duration::ZERO;
        for _ in 0..iters {
            let t0 = Instant::now();
            let stopped = Arc::new(AtomicU64::new(0));
            let mut addrs = Vec::with_capacity(actors);
            for _ in 0..actors {
                let addr = ActixAsyncFanoutActor {
                    mailbox_cap: cap,
                    stopped: Arc::clone(&stopped),
                }
                .start();
                addrs.push(addr);
            }
            for addr in addrs {
                addr.do_send(ActixAsyncFanoutMsg);
            }
            while stopped.load(Ordering::Acquire) < actors_u64 {
                tokio::task::yield_now().await;
            }
            measured += t0.elapsed();
        }
        measured
    })
}

fn run_af_async_io_single_actor(cfg: Config, iters: u64) -> Duration {
    let units = cfg.async_io_units.max(1);
    let cap = cfg.async_mailbox_cap.max(1);
    let sleep = Duration::from_micros(cfg.async_io_sleep_us.max(1));
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("failed to build tokio runtime");

    rt.block_on(async move {
        let completed = Arc::new(AtomicU64::new(0));
        let (addr, handle) = local_async::spawn_actor(
            cap,
            AfAsyncIoSerialActor {
                completed: Arc::clone(&completed),
                sleep,
            },
        );

        let warmup = units.min(32);
        completed.store(0, Ordering::Release);
        for _ in 0..warmup {
            loop {
                match addr.try_tell(()) {
                    Ok(()) => break,
                    Err(local_async::TryTellError::Full(())) => tokio::task::yield_now().await,
                    Err(local_async::TryTellError::Closed(())) => {
                        panic!("actor-framework async io serial warmup actor closed")
                    }
                }
            }
        }
        while completed.load(Ordering::Acquire) < warmup {
            tokio::task::yield_now().await;
        }

        let mut measured = Duration::ZERO;
        for _ in 0..iters {
            completed.store(0, Ordering::Release);
            let t0 = Instant::now();
            for _ in 0..units {
                loop {
                    match addr.try_tell(()) {
                        Ok(()) => break,
                        Err(local_async::TryTellError::Full(())) => tokio::task::yield_now().await,
                        Err(local_async::TryTellError::Closed(())) => {
                            panic!("actor-framework async io serial actor closed")
                        }
                    }
                }
            }
            while completed.load(Ordering::Acquire) < units {
                tokio::task::yield_now().await;
            }
            measured += t0.elapsed();
        }
        handle.shutdown().await;
        measured
    })
}

fn run_af_async_io_fanout(cfg: Config, iters: u64) -> Duration {
    let units = usize::try_from(cfg.async_io_units).unwrap_or(usize::MAX / 2);
    let units = units.max(1);
    let units_u64 = u64::try_from(units).unwrap_or(u64::MAX);
    let cap = cfg.spawn_mailbox_cap.max(1);
    let sleep = Duration::from_micros(cfg.async_io_sleep_us.max(1));
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("failed to build tokio runtime");

    rt.block_on(async move {
        let warmup_cycles = 1u64;
        for _ in 0..warmup_cycles {
            let completed = Arc::new(AtomicU64::new(0));
            let mut addrs = Vec::with_capacity(units);
            let mut handles = Vec::with_capacity(units);
            for _ in 0..units {
                let (addr, handle) = local_async::spawn_actor(
                    cap,
                    AfAsyncIoOneShotActor {
                        completed: Arc::clone(&completed),
                        sleep,
                    },
                );
                addrs.push(addr);
                handles.push(handle);
            }
            for addr in &addrs {
                loop {
                    match addr.try_tell(()) {
                        Ok(()) => break,
                        Err(local_async::TryTellError::Full(())) => tokio::task::yield_now().await,
                        Err(local_async::TryTellError::Closed(())) => {
                            panic!("actor-framework async io fanout warmup actor closed")
                        }
                    }
                }
            }
            drop(addrs);
            for handle in handles {
                handle.wait().await;
            }
            while completed.load(Ordering::Acquire) < units_u64 {
                tokio::task::yield_now().await;
            }
        }

        let mut measured = Duration::ZERO;
        for _ in 0..iters {
            let t0 = Instant::now();
            let completed = Arc::new(AtomicU64::new(0));
            let mut addrs = Vec::with_capacity(units);
            let mut handles = Vec::with_capacity(units);
            for _ in 0..units {
                let (addr, handle) = local_async::spawn_actor(
                    cap,
                    AfAsyncIoOneShotActor {
                        completed: Arc::clone(&completed),
                        sleep,
                    },
                );
                addrs.push(addr);
                handles.push(handle);
            }
            for addr in &addrs {
                loop {
                    match addr.try_tell(()) {
                        Ok(()) => break,
                        Err(local_async::TryTellError::Full(())) => tokio::task::yield_now().await,
                        Err(local_async::TryTellError::Closed(())) => {
                            panic!("actor-framework async io fanout actor closed")
                        }
                    }
                }
            }
            drop(addrs);
            for handle in handles {
                handle.wait().await;
            }
            while completed.load(Ordering::Acquire) < units_u64 {
                tokio::task::yield_now().await;
            }
            measured += t0.elapsed();
        }
        measured
    })
}

fn run_af_async_io_prespawned_fanout(cfg: Config, iters: u64) -> Duration {
    let units = usize::try_from(cfg.async_io_units).unwrap_or(usize::MAX / 2);
    let units = units.max(1);
    let units_u64 = u64::try_from(units).unwrap_or(u64::MAX);
    let cap = cfg.async_mailbox_cap.max(1);
    let sleep = Duration::from_micros(cfg.async_io_sleep_us.max(1));
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("failed to build tokio runtime");

    rt.block_on(async move {
        let completed = Arc::new(AtomicU64::new(0));
        let mut addrs = Vec::with_capacity(units);
        let mut handles = Vec::with_capacity(units);
        for _ in 0..units {
            let (addr, handle) = local_async::spawn_actor(
                cap,
                AfAsyncIoSerialActor {
                    completed: Arc::clone(&completed),
                    sleep,
                },
            );
            addrs.push(addr);
            handles.push(handle);
        }

        completed.store(0, Ordering::Release);
        for addr in &addrs {
            loop {
                match addr.try_tell(()) {
                    Ok(()) => break,
                    Err(local_async::TryTellError::Full(())) => tokio::task::yield_now().await,
                    Err(local_async::TryTellError::Closed(())) => {
                        panic!("actor-framework async io prespawned fanout warmup actor closed")
                    }
                }
            }
        }
        while completed.load(Ordering::Acquire) < units_u64 {
            tokio::task::yield_now().await;
        }

        let mut measured = Duration::ZERO;
        for _ in 0..iters {
            completed.store(0, Ordering::Release);
            let t0 = Instant::now();
            for addr in &addrs {
                loop {
                    match addr.try_tell(()) {
                        Ok(()) => break,
                        Err(local_async::TryTellError::Full(())) => tokio::task::yield_now().await,
                        Err(local_async::TryTellError::Closed(())) => {
                            panic!("actor-framework async io prespawned fanout actor closed")
                        }
                    }
                }
            }
            while completed.load(Ordering::Acquire) < units_u64 {
                tokio::task::yield_now().await;
            }
            measured += t0.elapsed();
        }

        for handle in handles {
            handle.shutdown().await;
        }
        measured
    })
}

fn run_af_async_work_single_actor(cfg: Config, iters: u64) -> Duration {
    let units = cfg.async_io_units.max(1);
    let cap = cfg.async_mailbox_cap.max(1);
    let work_iters = u32::try_from(cfg.async_work_iters)
        .unwrap_or(u32::MAX)
        .max(1);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("failed to build tokio runtime");

    rt.block_on(async move {
        let completed = Arc::new(AtomicU64::new(0));
        let (addr, handle) = local_async::spawn_actor(
            cap,
            AfAsyncWorkSerialActor {
                completed: Arc::clone(&completed),
                iters: work_iters,
            },
        );

        let warmup = units.min(32);
        completed.store(0, Ordering::Release);
        for _ in 0..warmup {
            loop {
                match addr.try_tell(()) {
                    Ok(()) => break,
                    Err(local_async::TryTellError::Full(())) => tokio::task::yield_now().await,
                    Err(local_async::TryTellError::Closed(())) => {
                        panic!("actor-framework async work serial warmup actor closed")
                    }
                }
            }
        }
        while completed.load(Ordering::Acquire) < warmup {
            tokio::task::yield_now().await;
        }

        let mut measured = Duration::ZERO;
        for _ in 0..iters {
            completed.store(0, Ordering::Release);
            let t0 = Instant::now();
            for _ in 0..units {
                loop {
                    match addr.try_tell(()) {
                        Ok(()) => break,
                        Err(local_async::TryTellError::Full(())) => tokio::task::yield_now().await,
                        Err(local_async::TryTellError::Closed(())) => {
                            panic!("actor-framework async work serial actor closed")
                        }
                    }
                }
            }
            while completed.load(Ordering::Acquire) < units {
                tokio::task::yield_now().await;
            }
            measured += t0.elapsed();
        }

        handle.shutdown().await;
        measured
    })
}

fn run_af_async_work_fanout(cfg: Config, iters: u64) -> Duration {
    let units = usize::try_from(cfg.async_io_units).unwrap_or(usize::MAX / 2);
    let units = units.max(1);
    let units_u64 = u64::try_from(units).unwrap_or(u64::MAX);
    let cap = cfg.spawn_mailbox_cap.max(1);
    let work_iters = u32::try_from(cfg.async_work_iters)
        .unwrap_or(u32::MAX)
        .max(1);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("failed to build tokio runtime");

    rt.block_on(async move {
        let warmup_cycles = 1u64;
        for _ in 0..warmup_cycles {
            let completed = Arc::new(AtomicU64::new(0));
            let mut addrs = Vec::with_capacity(units);
            let mut handles = Vec::with_capacity(units);
            for _ in 0..units {
                let (addr, handle) = local_async::spawn_actor(
                    cap,
                    AfAsyncWorkOneShotActor {
                        completed: Arc::clone(&completed),
                        iters: work_iters,
                    },
                );
                addrs.push(addr);
                handles.push(handle);
            }
            for addr in &addrs {
                loop {
                    match addr.try_tell(()) {
                        Ok(()) => break,
                        Err(local_async::TryTellError::Full(())) => tokio::task::yield_now().await,
                        Err(local_async::TryTellError::Closed(())) => {
                            panic!("actor-framework async work fanout warmup actor closed")
                        }
                    }
                }
            }
            drop(addrs);
            for handle in handles {
                handle.wait().await;
            }
            while completed.load(Ordering::Acquire) < units_u64 {
                tokio::task::yield_now().await;
            }
        }

        let mut measured = Duration::ZERO;
        for _ in 0..iters {
            let t0 = Instant::now();
            let completed = Arc::new(AtomicU64::new(0));
            let mut addrs = Vec::with_capacity(units);
            let mut handles = Vec::with_capacity(units);
            for _ in 0..units {
                let (addr, handle) = local_async::spawn_actor(
                    cap,
                    AfAsyncWorkOneShotActor {
                        completed: Arc::clone(&completed),
                        iters: work_iters,
                    },
                );
                addrs.push(addr);
                handles.push(handle);
            }
            for addr in &addrs {
                loop {
                    match addr.try_tell(()) {
                        Ok(()) => break,
                        Err(local_async::TryTellError::Full(())) => tokio::task::yield_now().await,
                        Err(local_async::TryTellError::Closed(())) => {
                            panic!("actor-framework async work fanout actor closed")
                        }
                    }
                }
            }
            drop(addrs);
            for handle in handles {
                handle.wait().await;
            }
            while completed.load(Ordering::Acquire) < units_u64 {
                tokio::task::yield_now().await;
            }
            measured += t0.elapsed();
        }
        measured
    })
}

fn run_af_async_work_prespawned_fanout(cfg: Config, iters: u64) -> Duration {
    let units = usize::try_from(cfg.async_io_units).unwrap_or(usize::MAX / 2);
    let units = units.max(1);
    let units_u64 = u64::try_from(units).unwrap_or(u64::MAX);
    let cap = cfg.async_mailbox_cap.max(1);
    let work_iters = u32::try_from(cfg.async_work_iters)
        .unwrap_or(u32::MAX)
        .max(1);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("failed to build tokio runtime");

    rt.block_on(async move {
        let completed = Arc::new(AtomicU64::new(0));
        let mut addrs = Vec::with_capacity(units);
        let mut handles = Vec::with_capacity(units);
        for _ in 0..units {
            let (addr, handle) = local_async::spawn_actor(
                cap,
                AfAsyncWorkSerialActor {
                    completed: Arc::clone(&completed),
                    iters: work_iters,
                },
            );
            addrs.push(addr);
            handles.push(handle);
        }

        completed.store(0, Ordering::Release);
        for addr in &addrs {
            loop {
                match addr.try_tell(()) {
                    Ok(()) => break,
                    Err(local_async::TryTellError::Full(())) => tokio::task::yield_now().await,
                    Err(local_async::TryTellError::Closed(())) => {
                        panic!("actor-framework async work prespawned fanout warmup actor closed")
                    }
                }
            }
        }
        while completed.load(Ordering::Acquire) < units_u64 {
            tokio::task::yield_now().await;
        }

        let mut measured = Duration::ZERO;
        for _ in 0..iters {
            completed.store(0, Ordering::Release);
            let t0 = Instant::now();
            for addr in &addrs {
                loop {
                    match addr.try_tell(()) {
                        Ok(()) => break,
                        Err(local_async::TryTellError::Full(())) => tokio::task::yield_now().await,
                        Err(local_async::TryTellError::Closed(())) => {
                            panic!("actor-framework async work prespawned fanout actor closed")
                        }
                    }
                }
            }
            while completed.load(Ordering::Acquire) < units_u64 {
                tokio::task::yield_now().await;
            }
            measured += t0.elapsed();
        }

        for handle in handles {
            handle.shutdown().await;
        }
        measured
    })
}

fn run_af_sync_ephemeral_pool(cfg: Config, iters: u64) -> Duration {
    let ops = cfg.ephemeral_ops;
    let cap = cfg.spawn_mailbox_cap;
    let processed = Arc::new(AtomicU64::new(0));
    let processed_for_make = Arc::clone(&processed);
    let pool = sync::default_pool::<AfSyncEphemeralActor>();
    let (addr, handle) = pool.spawn_reusable(cap, move || AfSyncEphemeralActor {
        processed: Arc::clone(&processed_for_make),
    });
    if !handle.wait_for_startup() {
        panic!("actor-framework reusable ephemeral pool failed before startup");
    }

    let warmup = ops.min(256);
    let mut token = 0u64;
    for _ in 0..warmup {
        token = token.wrapping_add(1);
        processed.store(0, Ordering::Release);
        let mut spins = 0u32;
        loop {
            match addr.try_tell(token) {
                Ok(()) => break,
                Err(_msg) => spin_then_yield(&mut spins),
            }
        }
        while processed.load(Ordering::Acquire) != token {
            spin_then_yield(&mut spins);
        }
        if !handle.restart_and_wait() {
            panic!("actor-framework sync ephemeral warmup restart failed");
        }
    }

    let mut measured = Duration::ZERO;
    for _ in 0..iters {
        for _ in 0..ops {
            token = token.wrapping_add(1);
            processed.store(0, Ordering::Release);
            let mut spins = 0u32;
            let t0 = Instant::now();
            loop {
                match addr.try_tell(token) {
                    Ok(()) => break,
                    Err(_msg) => spin_then_yield(&mut spins),
                }
            }
            while processed.load(Ordering::Acquire) != token {
                spin_then_yield(&mut spins);
            }
            if !handle.restart_and_wait() {
                panic!("actor-framework sync ephemeral restart failed");
            }
            measured += t0.elapsed();
        }
    }
    handle.shutdown();
    measured
}

fn run_af_sync_ephemeral_pool_direct(cfg: Config, iters: u64) -> Duration {
    let ops = cfg.ephemeral_ops;
    let processed = Arc::new(AtomicU64::new(0));
    let processed_for_make = Arc::clone(&processed);
    let mut actor = direct::Actor::new(move || AfSyncEphemeralActor {
        processed: Arc::clone(&processed_for_make),
    });

    if actor.state() != direct::ActorState::Running {
        panic!(
            "actor-framework local direct ephemeral actor did not start: state={:?}",
            actor.state()
        );
    }

    let warmup = ops.min(256);
    let mut token = 0u64;
    for _ in 0..warmup {
        token = token.wrapping_add(1);
        processed.store(0, Ordering::Release);
        actor
            .tell(token)
            .expect("actor-framework local direct ephemeral warmup tell failed");
        let mut spins = 0u32;
        while processed.load(Ordering::Acquire) != token {
            spin_then_yield(&mut spins);
        }
        if let Err(err) = actor.restart() {
            panic!("actor-framework local direct ephemeral warmup restart failed: {err:?}");
        }
    }

    let mut measured = Duration::ZERO;
    for _ in 0..iters {
        for _ in 0..ops {
            token = token.wrapping_add(1);
            processed.store(0, Ordering::Release);
            let mut spins = 0u32;
            let t0 = Instant::now();
            actor
                .tell(token)
                .expect("actor-framework local direct ephemeral tell failed");
            while processed.load(Ordering::Acquire) != token {
                spin_then_yield(&mut spins);
            }
            if let Err(err) = actor.restart() {
                panic!("actor-framework local direct ephemeral restart failed: {err:?}");
            }
            measured += t0.elapsed();
        }
    }
    actor.shutdown();
    measured
}

fn run_actix_sync_ephemeral_pool(cfg: Config, iters: u64) -> Duration {
    let ops = cfg.ephemeral_ops;
    let threads = cfg.actix_sync_threads;
    let sys = System::new();

    sys.block_on(async move {
        let pending_token = Arc::new(AtomicU64::new(0));
        let started_token = Arc::new(AtomicU64::new(u64::MAX));
        let pending_for_factory = Arc::clone(&pending_token);
        let started_for_factory = Arc::clone(&started_token);

        let addr = SyncArbiter::start(threads, move || ActixSyncSpawnActor {
            pending_token: Arc::clone(&pending_for_factory),
            started_token: Arc::clone(&started_for_factory),
        });

        while started_token.load(Ordering::Acquire) == u64::MAX {
            tokio::task::yield_now().await;
        }

        let mut token = 0u64;
        let warmup = ops.min(256);
        for i in 0..warmup {
            token = token.wrapping_add(1);
            loop {
                match addr.try_send(ActixEphemeralComputeMsg(token)) {
                    Ok(()) => break,
                    Err(ActixSendError::Full(_)) => tokio::task::yield_now().await,
                    Err(ActixSendError::Closed(_)) => panic!("actix sync ephemeral actor closed"),
                }
            }
            while started_token.load(Ordering::Acquire) != token {
                tokio::task::yield_now().await;
            }
            black_box(i);
        }

        let mut measured = Duration::ZERO;
        for _ in 0..iters {
            for _ in 0..ops {
                token = token.wrapping_add(1);
                let t0 = Instant::now();
                loop {
                    match addr.try_send(ActixEphemeralComputeMsg(token)) {
                        Ok(()) => break,
                        Err(ActixSendError::Full(_)) => tokio::task::yield_now().await,
                        Err(ActixSendError::Closed(_)) => {
                            panic!("actix sync ephemeral actor closed")
                        }
                    }
                }
                while started_token.load(Ordering::Acquire) != token {
                    tokio::task::yield_now().await;
                }
                measured += t0.elapsed();
            }
        }

        drop(addr);
        measured
    })
}

fn run_ractor_async_ephemeral_spawn_compute(cfg: Config, iters: u64) -> Duration {
    let ops = cfg.ephemeral_ops.max(1);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cfg.async_runtime_threads)
        .enable_time()
        .build()
        .expect("failed to build tokio runtime");

    rt.block_on(async move {
        let processed = Arc::new(AtomicU64::new(0));
        let mut token = 0u64;

        let warmup = ops.min(128);
        for _ in 0..warmup {
            token = token.wrapping_add(1);
            processed.store(0, Ordering::Release);
            let (addr, handle) = RactorEphemeralComputeActor::spawn(
                None,
                RactorEphemeralComputeActor,
                Arc::clone(&processed),
            )
            .await
            .expect("failed to spawn ractor ephemeral actor (warmup)");
            addr.send_message(token)
                .expect("ractor ephemeral warmup send failed");
            while processed.load(Ordering::Acquire) != token {
                tokio::task::yield_now().await;
            }
            handle.await.expect("ractor ephemeral warmup join failed");
        }

        let mut measured = Duration::ZERO;
        for _ in 0..iters {
            for _ in 0..ops {
                token = token.wrapping_add(1);
                processed.store(0, Ordering::Release);
                let t0 = Instant::now();
                let (addr, handle) = RactorEphemeralComputeActor::spawn(
                    None,
                    RactorEphemeralComputeActor,
                    Arc::clone(&processed),
                )
                .await
                .expect("failed to spawn ractor ephemeral actor");
                addr.send_message(token)
                    .expect("ractor ephemeral send failed");
                while processed.load(Ordering::Acquire) != token {
                    tokio::task::yield_now().await;
                }
                handle.await.expect("ractor ephemeral join failed");
                measured += t0.elapsed();
            }
        }
        measured
    })
}

fn run_af_sync_ephemeral_pool_ask(cfg: Config, iters: u64) -> Duration {
    let ops = cfg.ephemeral_ops;
    let cap = cfg.spawn_mailbox_cap;
    let pool = sync::default_pool::<AfSyncEphemeralAskActor>();
    let (addr, handle) = pool.spawn_reusable(cap, || AfSyncEphemeralAskActor);
    if !handle.wait_for_startup() {
        panic!("actor-framework reusable ephemeral ask pool failed before startup");
    }

    let mut token = 0u64;
    let warmup = ops.min(256);
    for _ in 0..warmup {
        token = token.wrapping_add(1);
        let out = sync::ask(&addr, |reply| AfSyncEphemeralAskMsg::Compute {
            token,
            reply,
        })
        .expect("actor-framework sync ephemeral ask warmup failed");
        if out != token.wrapping_add(1) {
            panic!(
                "actor-framework sync ephemeral ask warmup mismatch: got={}, expected={}",
                out,
                token.wrapping_add(1)
            );
        }
        if !handle.restart_and_wait() {
            panic!("actor-framework sync ephemeral ask warmup restart failed");
        }
    }

    let mut measured = Duration::ZERO;
    for _ in 0..iters {
        for _ in 0..ops {
            token = token.wrapping_add(1);
            let t0 = Instant::now();
            let out = sync::ask(&addr, |reply| AfSyncEphemeralAskMsg::Compute {
                token,
                reply,
            })
            .expect("actor-framework sync ephemeral ask failed");
            if out != token.wrapping_add(1) {
                panic!(
                    "actor-framework sync ephemeral ask mismatch: got={}, expected={}",
                    out,
                    token.wrapping_add(1)
                );
            }
            black_box(out);
            if !handle.restart_and_wait() {
                panic!("actor-framework sync ephemeral ask restart failed");
            }
            measured += t0.elapsed();
        }
    }
    handle.shutdown();
    measured
}

fn run_actix_sync_ephemeral_pool_ask(cfg: Config, iters: u64) -> Duration {
    let ops = cfg.ephemeral_ops;
    let threads = cfg.actix_sync_threads;
    let sys = System::new();

    sys.block_on(async move {
        let pending_token = Arc::new(AtomicU64::new(0));
        let started_token = Arc::new(AtomicU64::new(u64::MAX));
        let pending_for_factory = Arc::clone(&pending_token);
        let started_for_factory = Arc::clone(&started_token);

        let addr = SyncArbiter::start(threads, move || ActixSyncSpawnActor {
            pending_token: Arc::clone(&pending_for_factory),
            started_token: Arc::clone(&started_for_factory),
        });

        while started_token.load(Ordering::Acquire) == u64::MAX {
            tokio::task::yield_now().await;
        }

        let mut token = 0u64;
        let warmup = ops.min(256);
        for _ in 0..warmup {
            token = token.wrapping_add(1);
            let out = addr
                .send(ActixEphemeralComputeMsg(token))
                .await
                .expect("actix sync ephemeral ask warmup failed");
            if out != token.wrapping_add(1) {
                panic!(
                    "actix sync ephemeral ask warmup mismatch: got={}, expected={}",
                    out,
                    token.wrapping_add(1)
                );
            }
            while started_token.load(Ordering::Acquire) != token {
                tokio::task::yield_now().await;
            }
        }

        let mut measured = Duration::ZERO;
        for _ in 0..iters {
            for _ in 0..ops {
                token = token.wrapping_add(1);
                let t0 = Instant::now();
                let out = addr
                    .send(ActixEphemeralComputeMsg(token))
                    .await
                    .expect("actix sync ephemeral ask failed");
                if out != token.wrapping_add(1) {
                    panic!(
                        "actix sync ephemeral ask mismatch: got={}, expected={}",
                        out,
                        token.wrapping_add(1)
                    );
                }
                black_box(out);
                while started_token.load(Ordering::Acquire) != token {
                    tokio::task::yield_now().await;
                }
                measured += t0.elapsed();
            }
        }

        drop(addr);
        measured
    })
}

fn run_af_async_try_tell(cfg: Config, iters: u64) -> Duration {
    let ops = cfg.try_tell_ops;
    let cap = cfg.async_mailbox_cap;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("failed to build tokio runtime");

    rt.block_on(async move {
        let processed = Arc::new(AtomicU64::new(0));
        let (addr, handle) = local_async::spawn_actor(
            cap,
            AfAsyncTryTellActor {
                processed: Arc::clone(&processed),
            },
        );

        let warmup = ops.min(10_000);
        for _ in 0..warmup {
            loop {
                match addr.try_tell(1) {
                    Ok(()) => break,
                    Err(local_async::TryTellError::Full(_)) => tokio::task::yield_now().await,
                    Err(local_async::TryTellError::Closed(_)) => {
                        panic!("actor-framework async actor closed")
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
            for _ in 0..ops {
                loop {
                    match addr.try_tell(1) {
                        Ok(()) => break,
                        Err(local_async::TryTellError::Full(_)) => tokio::task::yield_now().await,
                        Err(local_async::TryTellError::Closed(_)) => {
                            panic!("actor-framework async actor closed")
                        }
                    }
                }
            }
            while processed.load(Ordering::Relaxed) < ops {
                tokio::task::yield_now().await;
            }
        }
        let elapsed = t0.elapsed();

        handle.shutdown().await;
        elapsed
    })
}

fn run_actix_async_try_tell(cfg: Config, iters: u64) -> Duration {
    let ops = cfg.try_tell_ops;
    let sys = System::new();

    sys.block_on(async move {
        let processed = Arc::new(AtomicU64::new(0));
        let addr = ActixAsyncTryTellActor {
            processed: Arc::clone(&processed),
            mailbox_cap: cfg.async_mailbox_cap,
        }
        .start();

        let warmup = ops.min(10_000);
        for _ in 0..warmup {
            loop {
                match addr.try_send(ActixTryTellMsg(1)) {
                    Ok(()) => break,
                    Err(ActixSendError::Full(_)) => tokio::task::yield_now().await,
                    Err(ActixSendError::Closed(_)) => panic!("actix async actor closed"),
                }
            }
        }
        while processed.load(Ordering::Relaxed) < warmup {
            tokio::task::yield_now().await;
        }

        let t0 = Instant::now();
        for _ in 0..iters {
            processed.store(0, Ordering::Relaxed);
            for _ in 0..ops {
                loop {
                    match addr.try_send(ActixTryTellMsg(1)) {
                        Ok(()) => break,
                        Err(ActixSendError::Full(_)) => tokio::task::yield_now().await,
                        Err(ActixSendError::Closed(_)) => panic!("actix async actor closed"),
                    }
                }
            }
            while processed.load(Ordering::Relaxed) < ops {
                tokio::task::yield_now().await;
            }
        }
        t0.elapsed()
    })
}

fn run_ractor_async_try_tell(cfg: Config, iters: u64) -> Duration {
    let ops = cfg.try_tell_ops;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("failed to build tokio runtime");

    rt.block_on(async move {
        let processed = Arc::new(AtomicU64::new(0));
        let (addr, handle) =
            RactorAsyncTellActor::spawn(None, RactorAsyncTellActor, Arc::clone(&processed))
                .await
                .expect("failed to spawn ractor async actor");

        let warmup = ops.min(10_000);
        for _ in 0..warmup {
            addr.send_message(1)
                .expect("ractor async tell warmup send failed");
        }
        while processed.load(Ordering::Relaxed) < warmup {
            tokio::task::yield_now().await;
        }

        let t0 = Instant::now();
        for _ in 0..iters {
            processed.store(0, Ordering::Relaxed);
            for _ in 0..ops {
                addr.send_message(1).expect("ractor async tell send failed");
            }
            while processed.load(Ordering::Relaxed) < ops {
                tokio::task::yield_now().await;
            }
        }
        let elapsed = t0.elapsed();
        addr.stop(None);
        handle.await.expect("ractor async actor join failed");
        elapsed
    })
}

fn run_af_async_tell(cfg: Config, iters: u64) -> Duration {
    let ops = cfg.try_tell_ops;
    let cap = cfg.async_mailbox_cap;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("failed to build tokio runtime");

    rt.block_on(async move {
        let processed = Arc::new(AtomicU64::new(0));
        let (addr, handle) = local_async::spawn_actor(
            cap,
            AfAsyncTryTellActor {
                processed: Arc::clone(&processed),
            },
        );

        let warmup = ops.min(10_000);
        for _ in 0..warmup {
            while !addr.tell(1) {
                tokio::task::yield_now().await;
            }
        }
        while processed.load(Ordering::Relaxed) < warmup {
            tokio::task::yield_now().await;
        }

        let t0 = Instant::now();
        for _ in 0..iters {
            processed.store(0, Ordering::Relaxed);
            for _ in 0..ops {
                while !addr.tell(1) {
                    tokio::task::yield_now().await;
                }
            }
            while processed.load(Ordering::Relaxed) < ops {
                tokio::task::yield_now().await;
            }
        }
        let elapsed = t0.elapsed();
        handle.shutdown().await;
        elapsed
    })
}

fn run_actix_async_tell(cfg: Config, iters: u64) -> Duration {
    let ops = cfg.try_tell_ops;
    let sys = System::new();

    sys.block_on(async move {
        let processed = Arc::new(AtomicU64::new(0));
        let addr = ActixAsyncTryTellActor {
            processed: Arc::clone(&processed),
            mailbox_cap: cfg.async_mailbox_cap,
        }
        .start();

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

fn run_af_async_ask(cfg: Config, iters: u64) -> Duration {
    let ops = cfg.ask_ops;
    let cap = cfg.async_mailbox_cap;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("failed to build tokio runtime");

    rt.block_on(async move {
        let (addr, handle) = local_async::spawn_actor(cap, AfAsyncAskActor);

        let warmup = ops.min(2_000);
        for i in 0..warmup {
            let out = local_async::ask::ask(&addr, |reply| AfAsyncAskMsg::Echo { v: i, reply })
                .await
                .expect("actor-framework async ask warmup failed");
            black_box(out);
        }

        let t0 = Instant::now();
        for _ in 0..iters {
            for i in 0..ops {
                let out = local_async::ask::ask(&addr, |reply| AfAsyncAskMsg::Echo { v: i, reply })
                    .await
                    .expect("actor-framework async ask failed");
                black_box(out);
            }
        }
        let elapsed = t0.elapsed();

        handle.shutdown().await;
        elapsed
    })
}

fn run_actix_async_ask(cfg: Config, iters: u64) -> Duration {
    let ops = cfg.ask_ops;
    let sys = System::new();

    sys.block_on(async move {
        let addr = ActixAsyncAskActor {
            mailbox_cap: cfg.async_mailbox_cap,
        }
        .start();

        let warmup = ops.min(2_000);
        for i in 0..warmup {
            let out = addr
                .send(ActixAskMsg(i))
                .await
                .expect("actix async ask warmup failed");
            black_box(out);
        }

        let t0 = Instant::now();
        for _ in 0..iters {
            for i in 0..ops {
                let out = addr
                    .send(ActixAskMsg(i))
                    .await
                    .expect("actix async ask failed");
                black_box(out);
            }
        }
        t0.elapsed()
    })
}

fn bench_compare_sync_try_tell(c: &mut Criterion, cfg: Config) {
    let mut group = c.benchmark_group("compare_sync_try_tell");
    group.sample_size(cfg.sample_size);
    group.warm_up_time(Duration::from_millis(cfg.warmup_ms.max(100)));
    group.measurement_time(Duration::from_millis(cfg.measurement_ms.max(500)));
    group.throughput(Throughput::Elements(cfg.try_tell_ops));

    group.bench_function("actor_framework_sync", move |b| {
        b.iter_custom(|iters| run_af_sync_try_tell(cfg, iters));
    });

    group.bench_function("actix_sync", move |b| {
        b.iter_custom(|iters| run_actix_sync_try_tell(cfg, iters));
    });

    group.finish();
}

fn bench_compare_sync_tell(c: &mut Criterion, cfg: Config) {
    let mut group = c.benchmark_group("compare_sync_tell");
    group.sample_size(cfg.sample_size);
    group.warm_up_time(Duration::from_millis(cfg.warmup_ms.max(100)));
    group.measurement_time(Duration::from_millis(cfg.measurement_ms.max(500)));
    group.throughput(Throughput::Elements(cfg.try_tell_ops));

    group.bench_function("actor_framework_sync", move |b| {
        b.iter_custom(|iters| run_af_sync_tell(cfg, iters));
    });

    group.bench_function("actix_sync", move |b| {
        b.iter_custom(|iters| run_actix_sync_tell(cfg, iters));
    });

    group.finish();
}

fn bench_compare_sync_ask(c: &mut Criterion, cfg: Config) {
    let mut group = c.benchmark_group("compare_sync_ask");
    group.sample_size(cfg.sample_size);
    group.warm_up_time(Duration::from_millis(cfg.warmup_ms.max(100)));
    group.measurement_time(Duration::from_millis(cfg.measurement_ms.max(500)));
    group.throughput(Throughput::Elements(cfg.ask_ops));

    group.bench_function("actor_framework_sync", move |b| {
        b.iter_custom(|iters| run_af_sync_ask(cfg, iters));
    });

    group.bench_function("actix_sync", move |b| {
        b.iter_custom(|iters| run_actix_sync_ask(cfg, iters));
    });

    group.finish();
}

fn bench_compare_af_sync_work_sharding(c: &mut Criterion, cfg: Config) {
    let mut group = c.benchmark_group("compare_af_sync_work_sharding");
    group.sample_size(cfg.sample_size);
    group.warm_up_time(Duration::from_millis(cfg.warmup_ms.max(100)));
    group.measurement_time(Duration::from_millis(cfg.measurement_ms.max(500)));
    group.throughput(Throughput::Elements(cfg.sync_work_ops));

    group.bench_function("af_single_actor_serial", move |b| {
        b.iter_custom(|iters| run_af_sync_work_single_actor(cfg, iters));
    });

    group.bench_function("af_sharded_pool", move |b| {
        b.iter_custom(|iters| run_af_sync_work_sharded_pool(cfg, iters));
    });

    group.bench_function("af_sharded_spawn_cycle", move |b| {
        b.iter_custom(|iters| run_af_sync_work_sharded_spawn_cycle(cfg, iters));
    });

    group.finish();
}

fn bench_compare_work_sharding_4x(c: &mut Criterion, cfg: Config) {
    let mut group = c.benchmark_group("compare_work_sharding_4x");
    group.sample_size(cfg.sample_size);
    group.warm_up_time(Duration::from_millis(cfg.warmup_ms.max(100)));
    group.measurement_time(Duration::from_millis(cfg.measurement_ms.max(500)));
    group.throughput(Throughput::Elements(cfg.sync_work_ops));

    let mut cfg_4x = cfg;
    cfg_4x.sync_work_shards = 4;

    group.bench_function("actor_framework_sync_1x", move |b| {
        b.iter_custom(|iters| run_af_sync_work_single_actor(cfg, iters));
    });

    group.bench_function("actor_framework_sync_4x", move |b| {
        b.iter_custom(|iters| run_af_sync_work_sharded_pool(cfg_4x, iters));
    });

    group.bench_function("actix_sync_1x", move |b| {
        b.iter_custom(|iters| run_actix_sync_work_single_actor(cfg, iters));
    });

    group.bench_function("actix_sync_4x", move |b| {
        b.iter_custom(|iters| run_actix_sync_work_sharded_4x(cfg, iters));
    });

    group.bench_function("ractor_async_1x", move |b| {
        b.iter_custom(|iters| run_ractor_async_work_single_actor(cfg, iters));
    });

    group.bench_function("ractor_async_4x", move |b| {
        b.iter_custom(|iters| run_ractor_async_work_sharded_4x(cfg, iters));
    });

    group.finish();
}

fn bench_compare_async_work_sharding_4x(c: &mut Criterion, cfg: Config) {
    let mut group = c.benchmark_group("compare_async_work_sharding_4x");
    group.sample_size(cfg.sample_size);
    group.warm_up_time(Duration::from_millis(cfg.warmup_ms.max(100)));
    group.measurement_time(Duration::from_millis(cfg.measurement_ms.max(500)));
    group.throughput(Throughput::Elements(cfg.sync_work_ops));

    group.bench_function("actor_framework_async_1x_mt4", move |b| {
        b.iter_custom(|iters| run_af_async_work_single_actor_mt4(cfg, iters));
    });

    group.bench_function("actor_framework_async_4x_mt4", move |b| {
        b.iter_custom(|iters| run_af_async_work_sharded_4x_mt4(cfg, iters));
    });

    group.bench_function("actix_async_1x_mt4", move |b| {
        b.iter_custom(|iters| run_actix_async_work_single_actor_mt4(cfg, iters));
    });

    group.bench_function("actix_async_4x_mt4", move |b| {
        b.iter_custom(|iters| run_actix_async_work_sharded_4x_mt4(cfg, iters));
    });
    group.bench_function("ractor_async_1x_mt4", move |b| {
        b.iter_custom(|iters| run_ractor_async_work_single_actor(cfg, iters));
    });

    group.bench_function("ractor_async_4x_mt4", move |b| {
        b.iter_custom(|iters| run_ractor_async_work_sharded_4x(cfg, iters));
    });

    group.finish();
}

fn bench_compare_pubsub_local(c: &mut Criterion, cfg: Config) {
    let mut group = c.benchmark_group("compare_pubsub_local");
    group.sample_size(cfg.sample_size);
    group.warm_up_time(Duration::from_millis(cfg.warmup_ms.max(100)));
    group.measurement_time(Duration::from_millis(cfg.measurement_ms.max(500)));
    group.throughput(Throughput::Elements(cfg.pubsub_msgs));

    group.bench_function("actor_framework_async", move |b| {
        b.iter_custom(|iters| run_af_async_pubsub_local(cfg, iters));
    });

    group.bench_function("actix_async_loop_tell", move |b| {
        b.iter_custom(|iters| run_actix_async_pubsub_loop_tell(cfg, iters));
    });
    group.bench_function("ractor_async", move |b| {
        b.iter_custom(|iters| run_ractor_async_pubsub_local(cfg, iters));
    });

    group.finish();
}

fn bench_compare_broadcast_sync_local(c: &mut Criterion, cfg: Config) {
    let mut group = c.benchmark_group("compare_broadcast_sync_local");
    group.sample_size(cfg.sample_size);
    group.warm_up_time(Duration::from_millis(cfg.warmup_ms.max(100)));
    group.measurement_time(Duration::from_millis(cfg.measurement_ms.max(500)));
    group.throughput(Throughput::Elements(cfg.broadcast_msgs));

    group.bench_function("actor_framework_sync", move |b| {
        b.iter_custom(|iters| run_af_sync_broadcast_local(cfg, iters));
    });

    group.bench_function("actor_framework_sync_loop_tell", move |b| {
        b.iter_custom(|iters| run_af_sync_broadcast_loop_tell(cfg, iters));
    });

    group.bench_function("actix_sync_loop_tell", move |b| {
        b.iter_custom(|iters| run_actix_sync_broadcast_loop_tell(cfg, iters));
    });

    group.finish();
}

fn bench_compare_broadcast_async_local(c: &mut Criterion, cfg: Config) {
    let mut group = c.benchmark_group("compare_broadcast_async_local");
    group.sample_size(cfg.sample_size);
    group.warm_up_time(Duration::from_millis(cfg.warmup_ms.max(100)));
    group.measurement_time(Duration::from_millis(cfg.measurement_ms.max(500)));
    group.throughput(Throughput::Elements(cfg.broadcast_msgs));

    group.bench_function("actor_framework_async", move |b| {
        b.iter_custom(|iters| run_af_async_broadcast_local(cfg, iters));
    });

    group.bench_function("actor_framework_async_loop_tell", move |b| {
        b.iter_custom(|iters| run_af_async_broadcast_loop_tell(cfg, iters));
    });

    group.bench_function("actix_async_loop_tell", move |b| {
        b.iter_custom(|iters| run_actix_async_broadcast_loop_tell(cfg, iters));
    });
    group.bench_function("ractor_async", move |b| {
        b.iter_custom(|iters| run_ractor_async_broadcast_local(cfg, iters));
    });

    group.finish();
}

fn bench_compare_async_try_tell(c: &mut Criterion, cfg: Config) {
    let mut group = c.benchmark_group("compare_async_try_tell");
    group.sample_size(cfg.sample_size);
    group.warm_up_time(Duration::from_millis(cfg.warmup_ms.max(100)));
    group.measurement_time(Duration::from_millis(cfg.measurement_ms.max(500)));
    group.throughput(Throughput::Elements(cfg.try_tell_ops));

    group.bench_function("actor_framework_async", move |b| {
        b.iter_custom(|iters| run_af_async_try_tell(cfg, iters));
    });

    group.bench_function("actix_async", move |b| {
        b.iter_custom(|iters| run_actix_async_try_tell(cfg, iters));
    });
    group.bench_function("ractor_async", move |b| {
        b.iter_custom(|iters| run_ractor_async_try_tell(cfg, iters));
    });

    group.finish();
}

fn bench_compare_async_tell(c: &mut Criterion, cfg: Config) {
    let mut group = c.benchmark_group("compare_async_tell");
    group.sample_size(cfg.sample_size);
    group.warm_up_time(Duration::from_millis(cfg.warmup_ms.max(100)));
    group.measurement_time(Duration::from_millis(cfg.measurement_ms.max(500)));
    group.throughput(Throughput::Elements(cfg.try_tell_ops));

    group.bench_function("actor_framework_async", move |b| {
        b.iter_custom(|iters| run_af_async_tell(cfg, iters));
    });

    group.bench_function("actix_async", move |b| {
        b.iter_custom(|iters| run_actix_async_tell(cfg, iters));
    });
    group.bench_function("ractor_async", move |b| {
        b.iter_custom(|iters| run_ractor_async_try_tell(cfg, iters));
    });

    group.finish();
}

fn bench_compare_async_ask(c: &mut Criterion, cfg: Config) {
    let mut group = c.benchmark_group("compare_async_ask");
    group.sample_size(cfg.sample_size);
    group.warm_up_time(Duration::from_millis(cfg.warmup_ms.max(100)));
    group.measurement_time(Duration::from_millis(cfg.measurement_ms.max(500)));
    group.throughput(Throughput::Elements(cfg.ask_ops));

    group.bench_function("actor_framework_async", move |b| {
        b.iter_custom(|iters| run_af_async_ask(cfg, iters));
    });

    group.bench_function("actix_async", move |b| {
        b.iter_custom(|iters| run_actix_async_ask(cfg, iters));
    });

    group.finish();
}

fn bench_compare_sync_spawn_pool(c: &mut Criterion, cfg: Config) {
    let mut group = c.benchmark_group("compare_sync_spawn_pool");
    group.sample_size(cfg.sample_size);
    group.warm_up_time(Duration::from_millis(cfg.warmup_ms.max(100)));
    group.measurement_time(Duration::from_millis(cfg.measurement_ms.max(500)));
    group.throughput(Throughput::Elements(cfg.spawn_ops));

    group.bench_function("actor_framework_sync", move |b| {
        b.iter_custom(|iters| run_af_sync_spawn_pool(cfg, iters));
    });

    group.bench_function("actix_sync", move |b| {
        b.iter_custom(|iters| run_actix_sync_spawn_pool(cfg, iters));
    });

    group.finish();
}

fn bench_compare_sync_respawn_pool(c: &mut Criterion, cfg: Config) {
    let mut group = c.benchmark_group("compare_sync_respawn_pool");
    group.sample_size(cfg.sample_size);
    group.warm_up_time(Duration::from_millis(cfg.warmup_ms.max(100)));
    group.measurement_time(Duration::from_millis(cfg.measurement_ms.max(500)));
    group.throughput(Throughput::Elements(cfg.spawn_ops));

    group.bench_function("actor_framework_sync", move |b| {
        b.iter_custom(|iters| run_af_sync_respawn_pool(cfg, iters));
    });

    group.bench_function("actix_sync", move |b| {
        b.iter_custom(|iters| run_actix_sync_spawn_pool(cfg, iters));
    });

    group.finish();
}

fn bench_compare_async_spawn(c: &mut Criterion, cfg: Config) {
    let mut group = c.benchmark_group("compare_async_spawn");
    group.sample_size(cfg.sample_size);
    group.warm_up_time(Duration::from_millis(cfg.warmup_ms.max(100)));
    group.measurement_time(Duration::from_millis(cfg.measurement_ms.max(500)));
    group.throughput(Throughput::Elements(cfg.spawn_ops));

    group.bench_function("actor_framework_async", move |b| {
        b.iter_custom(|iters| run_af_async_spawn(cfg, iters));
    });

    group.bench_function("actix_async", move |b| {
        b.iter_custom(|iters| run_actix_async_spawn(cfg, iters));
    });

    group.bench_function("ractor_async", move |b| {
        b.iter_custom(|iters| run_ractor_async_spawn(cfg, iters));
    });
    group.finish();
}

fn bench_compare_async_fanout_shutdown(c: &mut Criterion, cfg: Config) {
    let mut group = c.benchmark_group("compare_async_fanout_shutdown");
    group.sample_size(cfg.sample_size);
    group.warm_up_time(Duration::from_millis(cfg.warmup_ms.max(100)));
    group.measurement_time(Duration::from_millis(cfg.measurement_ms.max(500)));
    group.throughput(Throughput::Elements(cfg.async_fanout_actors));

    group.bench_function("actor_framework_async", move |b| {
        b.iter_custom(|iters| run_af_async_fanout_shutdown(cfg, iters));
    });

    group.bench_function("actix_async", move |b| {
        b.iter_custom(|iters| run_actix_async_fanout_shutdown(cfg, iters));
    });
    group.finish();
}

fn bench_compare_af_async_io_seriality(c: &mut Criterion, cfg: Config) {
    let mut group = c.benchmark_group("compare_af_async_io_seriality");
    group.sample_size(cfg.sample_size);
    group.warm_up_time(Duration::from_millis(cfg.warmup_ms.max(100)));
    group.measurement_time(Duration::from_millis(cfg.measurement_ms.max(500)));
    group.throughput(Throughput::Elements(cfg.async_io_units));

    group.bench_function("af_single_actor_serial", move |b| {
        b.iter_custom(|iters| run_af_async_io_single_actor(cfg, iters));
    });

    group.bench_function("af_many_actors_fanout", move |b| {
        b.iter_custom(|iters| run_af_async_io_fanout(cfg, iters));
    });

    group.bench_function("af_many_actors_prespawned", move |b| {
        b.iter_custom(|iters| run_af_async_io_prespawned_fanout(cfg, iters));
    });

    group.finish();
}

fn bench_compare_af_async_work_seriality(c: &mut Criterion, cfg: Config) {
    let mut group = c.benchmark_group("compare_af_async_work_seriality");
    group.sample_size(cfg.sample_size);
    group.warm_up_time(Duration::from_millis(cfg.warmup_ms.max(100)));
    group.measurement_time(Duration::from_millis(cfg.measurement_ms.max(500)));
    group.throughput(Throughput::Elements(cfg.async_io_units));

    group.bench_function("af_single_actor_serial", move |b| {
        b.iter_custom(|iters| run_af_async_work_single_actor(cfg, iters));
    });

    group.bench_function("af_many_actors_fanout", move |b| {
        b.iter_custom(|iters| run_af_async_work_fanout(cfg, iters));
    });

    group.bench_function("af_many_actors_prespawned", move |b| {
        b.iter_custom(|iters| run_af_async_work_prespawned_fanout(cfg, iters));
    });

    group.finish();
}

fn bench_compare_ephemeral_pool_compute(c: &mut Criterion, cfg: Config) {
    let mut group = c.benchmark_group("compare_ephemeral_pool_compute");
    group.sample_size(cfg.sample_size);
    group.warm_up_time(Duration::from_millis(cfg.warmup_ms.max(100)));
    group.measurement_time(Duration::from_millis(cfg.measurement_ms.max(500)));
    group.throughput(Throughput::Elements(cfg.ephemeral_ops));

    group.bench_function("actor_framework_sync", move |b| {
        b.iter_custom(|iters| run_af_sync_ephemeral_pool(cfg, iters));
    });

    group.bench_function("actix_sync", move |b| {
        b.iter_custom(|iters| run_actix_sync_ephemeral_pool(cfg, iters));
    });
    group.bench_function("ractor_async_spawn_compute", move |b| {
        b.iter_custom(|iters| run_ractor_async_ephemeral_spawn_compute(cfg, iters));
    });

    group.finish();
}

fn bench_compare_ephemeral_pool_compute_direct(c: &mut Criterion, cfg: Config) {
    let mut group = c.benchmark_group("compare_ephemeral_pool_compute_direct");
    group.sample_size(cfg.sample_size);
    group.warm_up_time(Duration::from_millis(cfg.warmup_ms.max(100)));
    group.measurement_time(Duration::from_millis(cfg.measurement_ms.max(500)));
    group.throughput(Throughput::Elements(cfg.ephemeral_ops));

    group.bench_function("actor_framework_sync_direct", move |b| {
        b.iter_custom(|iters| run_af_sync_ephemeral_pool_direct(cfg, iters));
    });

    group.bench_function("actix_sync", move |b| {
        b.iter_custom(|iters| run_actix_sync_ephemeral_pool(cfg, iters));
    });
    group.bench_function("ractor_async_spawn_compute", move |b| {
        b.iter_custom(|iters| run_ractor_async_ephemeral_spawn_compute(cfg, iters));
    });

    group.finish();
}

fn bench_compare_ephemeral_pool_compute_ask(c: &mut Criterion, cfg: Config) {
    let mut group = c.benchmark_group("compare_ephemeral_pool_compute_ask");
    group.sample_size(cfg.sample_size);
    group.warm_up_time(Duration::from_millis(cfg.warmup_ms.max(100)));
    group.measurement_time(Duration::from_millis(cfg.measurement_ms.max(500)));
    group.throughput(Throughput::Elements(cfg.ephemeral_ops));

    group.bench_function("actor_framework_sync", move |b| {
        b.iter_custom(|iters| run_af_sync_ephemeral_pool_ask(cfg, iters));
    });

    group.bench_function("actix_sync", move |b| {
        b.iter_custom(|iters| run_actix_sync_ephemeral_pool_ask(cfg, iters));
    });
    group.finish();
}

fn benchmark(c: &mut Criterion) {
    let cfg = Config::from_env();
    let try_tell_ops_usize = usize::try_from(cfg.try_tell_ops).unwrap_or(usize::MAX);
    if cfg.sync_mailbox_cap < try_tell_ops_usize {
        eprintln!(
            "WARNING: sync_mailbox_cap ({}) < try_tell_ops ({}); AF sync try_tell/tell will backpressure while Actix sync is effectively unbounded.",
            cfg.sync_mailbox_cap, cfg.try_tell_ops
        );
    }
    let sync_work_ops_usize = usize::try_from(cfg.sync_work_ops).unwrap_or(usize::MAX);
    if cfg.async_work_mailbox_cap < sync_work_ops_usize {
        eprintln!(
            "WARNING: async_work_mailbox_cap ({}) < sync_work_ops ({}); AF async work sharding lane may backpressure while Ractor is effectively unbounded.",
            cfg.async_work_mailbox_cap, cfg.sync_work_ops
        );
    }
    match sync::set_default_pool_config(sync::PoolConfig {
        size: cfg.af_sync_pool_size,
        spin_init: cfg.af_sync_pool_spin_init,
    }) {
        Ok(()) => {}
        Err(existing) => {
            if existing.size != cfg.af_sync_pool_size
                || existing.spin_init != cfg.af_sync_pool_spin_init
            {
                panic!(
                    "sync default pool already initialized with size={} spin_init={}, requested size={} spin_init={}",
                    existing.size,
                    existing.spin_init,
                    cfg.af_sync_pool_size,
                    cfg.af_sync_pool_spin_init
                );
            }
        }
    }
    if cfg.af_sync_pool_size != cfg.actix_sync_threads {
        eprintln!(
            "WARNING: af_sync_pool_size ({}) != actix_sync_threads ({}); comparisons are not apples-to-apples.",
            cfg.af_sync_pool_size, cfg.actix_sync_threads
        );
    }
    eprintln!(
        "actix_compare_criterion config: try_tell_ops={} ask_ops={} spawn_ops={} ephemeral_ops={} async_fanout_actors={} async_io_units={} async_io_sleep_us={} async_work_iters={} async_rt_threads={} sync_work_ops={} sync_work_iters={} sync_work_shards={} pubsub_topics={} pubsub_subs_per_topic={} pubsub_msgs={} pubsub_warmup={} pubsub_mailbox_cap={} pubsub_rt_threads={} pubsub_hot_topic={} pubsub_stripes={} pubsub_threshold={} broadcast_subs={} broadcast_msgs={} broadcast_warmup={} broadcast_mailbox_cap={} broadcast_stripes={} broadcast_threshold={} sync_mailbox_cap={} async_mailbox_cap={} async_work_mailbox_cap={} spawn_mailbox_cap={} actix_sync_threads={} af_sync_pool_size={} af_sync_pool_spin_init={} sample_size={} warmup_ms={} measurement_ms={} tell_msg_bytes={{af:{},actix:{},ractor:{}}}",
        cfg.try_tell_ops,
        cfg.ask_ops,
        cfg.spawn_ops,
        cfg.ephemeral_ops,
        cfg.async_fanout_actors,
        cfg.async_io_units,
        cfg.async_io_sleep_us,
        cfg.async_work_iters,
        cfg.async_runtime_threads,
        cfg.sync_work_ops,
        cfg.sync_work_iters,
        cfg.sync_work_shards,
        cfg.pubsub_topics,
        cfg.pubsub_subs_per_topic,
        cfg.pubsub_msgs,
        cfg.pubsub_warmup,
        cfg.pubsub_mailbox_cap,
        cfg.pubsub_runtime_threads,
        cfg.pubsub_hot_topic,
        cfg.pubsub_stripes,
        cfg.pubsub_threshold,
        cfg.broadcast_subs,
        cfg.broadcast_msgs,
        cfg.broadcast_warmup,
        cfg.broadcast_mailbox_cap,
        cfg.broadcast_stripes,
        cfg.broadcast_threshold,
        cfg.sync_mailbox_cap,
        cfg.async_mailbox_cap,
        cfg.async_work_mailbox_cap,
        cfg.spawn_mailbox_cap,
        cfg.actix_sync_threads,
        cfg.af_sync_pool_size,
        cfg.af_sync_pool_spin_init,
        cfg.sample_size,
        cfg.warmup_ms,
        cfg.measurement_ms,
        std::mem::size_of::<u64>(),
        std::mem::size_of::<ActixTryTellMsg>(),
        std::mem::size_of::<u64>()
    );
    eprintln!(
        "spawn_pool semantics: AF=sync::spawn_actor pooled spawn-cycle-to-ready (spawn+ready+stop/return); Actix=SyncContext::stop-triggered pooled respawn-to-ready (prestarted SyncArbiter, no thread creation in timed region)"
    );
    eprintln!(
        "respawn_pool semantics: AF=ContextPool::spawn_reusable restart-to-ready (reused mailbox/control on leased context); Actix=SyncContext::stop-triggered pooled respawn-to-ready"
    );
    eprintln!(
        "af_sync_work_sharding semantics: AF only; micro-compute sync workload measured as (a) single serial actor, (b) pre-spawned sharded pool (max throughput path), (c) sharded spawn-cycle including spawn overhead."
    );
    eprintln!(
        "work_sharding_4x semantics: same micro-compute workload (sync_work_iters) measured as 1x vs 4x sharded actors. AF/Actix lanes are sync runtimes; Ractor lanes are async runtime reference."
    );
    eprintln!(
        "async_work_sharding_4x semantics: apples-to-apples async sharding on 4-worker Tokio runtime; measures same micro-compute workload (sync_work_iters) as 1x vs 4x sharded actors for AF/Actix/Ractor."
    );
    eprintln!(
        "pubsub_local semantics: publish cfg.pubsub_msgs messages over cfg.pubsub_topics topics (hot_topic configurable), each message fan-outs to cfg.pubsub_subs_per_topic subscribers; throughput is publish ops/s while waiting for full subscriber delivery. AF uses local_async::PubSub native async-topic fast path (`topic_async` + `publish_fast_to_async`) with optional mixed sync subscriptions available, Actix uses async looped-tell fallback, Ractor uses OutputPort."
    );
    eprintln!(
        "broadcast_sync_local semantics: end-to-end publish+delivery throughput for cfg.broadcast_msgs to cfg.broadcast_subs subscribers; AF lane uses cached BroadcastPublisher::publish_fast_stable dispatch (adaptive concurrent fanout), plus AF/Actix sync looped-tell baselines."
    );
    eprintln!(
        "broadcast_async_local semantics: end-to-end publish+delivery throughput for cfg.broadcast_msgs to cfg.broadcast_subs subscribers. AF native lane uses cached BroadcastPublisher::publish_fast_stable dispatch (adaptive concurrent fanout) plus async looped-tell baseline; Actix uses async looped tells; Ractor uses OutputPort."
    );
    eprintln!(
        "ephemeral_pool_compute semantics: AF=ContextPool::spawn_reusable tell-compute + restart-to-ready; Actix=SyncArbiter tell-compute + stop-triggered respawn; Ractor=spawn actor + tell compute + self-stop + join (no built-in sync pool lane)."
    );
    eprintln!(
        "ephemeral_pool_compute_direct semantics: AF=local_direct::Actor tell-compute + restart (caller-thread local direct runtime, no mailbox hop); Actix=SyncArbiter tell-compute + stop-triggered respawn; Ractor=spawn actor + tell compute + self-stop + join (no built-in sync pool lane)."
    );
    eprintln!(
        "ephemeral_pool_compute_ask semantics: AF=ContextPool::spawn_reusable ask-compute(reply) + restart-to-ready; Actix=SyncArbiter ask-compute(reply) + stop-triggered respawn."
    );
    eprintln!(
        "async_spawn semantics: AF=local_async::spawn_actor spawn-to-ready cycle (spawn+on_start+shutdown); Actix=Context::start spawn-to-ready cycle (start+started+stop msg); Ractor=Actor::spawn unbounded spawn-to-ready cycle (spawn+pre_start+stop)"
    );
    eprintln!(
        "async_fanout_shutdown semantics: Spawn N async actors (default N=1000), send each one close message, and wait for actor stop; AF stops in-handler via AsyncContext::request_shutdown, Actix stop via handler context."
    );
    eprintln!(
        "af_async_io_seriality semantics: AF only; same IO workload per unit (sleep async_io_sleep_us) measured as (a) one serial actor processing N messages, (b) N actors processing one message each then self-stopping, (c) N pre-spawned actors processing one message each."
    );
    eprintln!(
        "af_async_work_seriality semantics: AF only; same shape as io_seriality but per-message does deterministic micro-compute (async_work_iters) instead of sleep."
    );
    eprintln!(
        "ractor tell semantics: ActorRef::send_message on unbounded mailbox (no Full/backpressure signal)."
    );

    bench_compare_sync_try_tell(c, cfg);
    bench_compare_sync_tell(c, cfg);
    bench_compare_sync_ask(c, cfg);
    bench_compare_af_sync_work_sharding(c, cfg);
    bench_compare_work_sharding_4x(c, cfg);
    bench_compare_async_work_sharding_4x(c, cfg);
    bench_compare_pubsub_local(c, cfg);
    bench_compare_broadcast_sync_local(c, cfg);
    bench_compare_broadcast_async_local(c, cfg);
    bench_compare_sync_spawn_pool(c, cfg);
    bench_compare_sync_respawn_pool(c, cfg);
    bench_compare_ephemeral_pool_compute(c, cfg);
    bench_compare_ephemeral_pool_compute_direct(c, cfg);
    bench_compare_ephemeral_pool_compute_ask(c, cfg);
    bench_compare_async_spawn(c, cfg);
    bench_compare_async_fanout_shutdown(c, cfg);
    bench_compare_af_async_io_seriality(c, cfg);
    bench_compare_af_async_work_seriality(c, cfg);
    bench_compare_async_try_tell(c, cfg);
    bench_compare_async_tell(c, cfg);
    bench_compare_async_ask(c, cfg);
}

criterion_group! {
    name = benches;
    config = Criterion::default().configure_from_args();
    targets = benchmark
}
criterion_main!(benches);
