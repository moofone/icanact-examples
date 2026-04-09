use icanact_core::local_async::{self, AsyncActor};
use icanact_core::local_sync::{self, SelfShutdownToken};
use std::env;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug)]
struct Config {
    msgs: u64,
    warmup: u64,
    cap: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            msgs: 1_000_000,
            warmup: 10_000,
            cap: 16_384,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct Start(u64);

impl icanact_core::TellAskTell for Start {}

struct ReceiverActor {
    target: u64,
    processed: Arc<AtomicU64>,
    checksum: Arc<AtomicU64>,
    done: Arc<AtomicBool>,
    shutdown: Option<SelfShutdownToken>,
}

impl local_sync::SyncActor for ReceiverActor {
    type Contract = local_sync::contract::TellOnly;
    type Tell = ();
    type Ask = ();
    type Reply = ();
    type Channel = u64;
    type PubSub = ();
    type Broadcast = ();

    fn on_sync_start(&mut self, shutdown: SelfShutdownToken) {
        self.shutdown = Some(shutdown);
    }

    fn handle_channel(&mut self, _channel_id: local_sync::ChannelId, msg: Self::Channel) {
        let processed = self.processed.fetch_add(1, Ordering::AcqRel) + 1;
        self.checksum.fetch_add(msg, Ordering::AcqRel);
        if processed == self.target {
            self.done.store(true, Ordering::Release);
            self.shutdown
                .as_ref()
                .expect("receiver shutdown token")
                .shutdown();
        }
    }
}

struct AsyncSenderActor {
    channel: local_sync::ChannelSender<u64>,
}

impl AsyncActor for AsyncSenderActor {
    type Contract = local_async::contract::TellOnly;
    type Tell = Start;
    type Ask = ();
    type Reply = ();
    type Channel = ();
    type PubSub = ();
    type Broadcast = ();

    async fn handle_tell(&mut self, msg: Self::Tell) {
        for seq in 1..=msg.0 {
            loop {
                if self.channel.try_send(seq).is_ok() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        }
    }
}

fn usage() -> &'static str {
    "Usage: cargo bench --bench spsc_async_to_sync_actor -- [options]\n\
     \n\
     Environment overrides:\n\
       AF_SPSC_ASYNC_TO_SYNC_MSGS    Iterations (default: 1000000)\n\
       AF_SPSC_ASYNC_TO_SYNC_WARMUP  Warmup iterations (default: 10000)\n\
       AF_SPSC_ASYNC_TO_SYNC_CAP     Channel capacity floor (default: 16384)\n\
     \n\
     CLI options:\n\
       --msgs N\n\
       --warmup N\n\
       --cap N\n"
}

fn parse_u64(raw: &str, flag: &str) -> Result<u64, String> {
    raw.parse::<u64>()
        .map_err(|err| format!("invalid {flag} value '{raw}': {err}"))
}

fn parse_usize(raw: &str, flag: &str) -> Result<usize, String> {
    raw.parse::<usize>()
        .map_err(|err| format!("invalid {flag} value '{raw}': {err}"))
}

fn parse_u64_env(key: &str, default: u64) -> u64 {
    match env::var(key) {
        Ok(v) => v.parse::<u64>().unwrap_or(default),
        Err(_) => default,
    }
}

fn parse_usize_env(key: &str, default: usize) -> usize {
    match env::var(key) {
        Ok(v) => v.parse::<usize>().unwrap_or(default),
        Err(_) => default,
    }
}

fn parse_config() -> Result<Config, String> {
    let mut cfg = Config {
        msgs: parse_u64_env("AF_SPSC_ASYNC_TO_SYNC_MSGS", 1_000_000).max(1),
        warmup: parse_u64_env("AF_SPSC_ASYNC_TO_SYNC_WARMUP", 10_000),
        cap: parse_usize_env("AF_SPSC_ASYNC_TO_SYNC_CAP", 16_384).max(1),
    };
    let mut it = env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--help" | "-h" => return Err(usage().to_string()),
            "--bench" => {}
            "--msgs" => {
                cfg.msgs = parse_u64(
                    &it.next()
                        .ok_or_else(|| "missing value for --msgs".to_string())?,
                    "--msgs",
                )?
                .max(1);
            }
            "--warmup" => {
                cfg.warmup = parse_u64(
                    &it.next()
                        .ok_or_else(|| "missing value for --warmup".to_string())?,
                    "--warmup",
                )?;
            }
            "--cap" => {
                cfg.cap = parse_usize(
                    &it.next()
                        .ok_or_else(|| "missing value for --cap".to_string())?,
                    "--cap",
                )?
                .max(1);
            }
            other => return Err(format!("unknown arg: {other}\n\n{}", usage())),
        }
    }
    Ok(cfg)
}

fn effective_cap(cfg: Config, count: u64) -> usize {
    usize::try_from(count)
        .map(|count| cfg.cap.max(count.max(1)))
        .unwrap_or(usize::MAX)
}

fn expected_checksum(total: u64) -> u64 {
    total.saturating_mul(total.saturating_add(1)) / 2
}

fn wait_done(done: &AtomicBool, label: &str) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !done.load(Ordering::Acquire) {
        assert!(Instant::now() < deadline, "timeout waiting for {label}");
        std::hint::spin_loop();
        std::thread::yield_now();
    }
}

async fn run_once(total: u64, cap: usize) -> Duration {
    let done = Arc::new(AtomicBool::new(false));
    let processed = Arc::new(AtomicU64::new(0));
    let checksum = Arc::new(AtomicU64::new(0));

    let (receiver_ref, receiver_handle) = local_sync::spawn_with_opts(
        ReceiverActor {
            target: total,
            processed: Arc::clone(&processed),
            checksum: Arc::clone(&checksum),
            done: Arc::clone(&done),
            shutdown: None,
        },
        local_sync::SpawnOpts {
            mailbox_capacity: cap,
            ..Default::default()
        },
    );
    receiver_handle.wait_for_startup();
    let channel = receiver_ref
        .add_channel("bench", cap)
        .expect("receiver channel");

    let (sender_ref, sender_handle) = local_async::spawn_with_opts(
        AsyncSenderActor { channel },
        local_async::SpawnOpts {
            mailbox_capacity: 1,
            ..Default::default()
        },
    )
    .await;
    assert!(
        sender_handle.wait_for_startup(Duration::from_secs(1)).await,
        "async sender should start"
    );

    let start = Instant::now();
    assert!(sender_ref.tell(Start(total)), "async sender kickoff should enqueue");
    wait_done(done.as_ref(), "async->sync receiver completion");
    let elapsed = start.elapsed();

    assert_eq!(processed.load(Ordering::Acquire), total);
    assert_eq!(checksum.load(Ordering::Acquire), expected_checksum(total));

    sender_handle.shutdown().await;
    receiver_handle.shutdown();
    elapsed
}

fn main() {
    let cfg = match parse_config() {
        Ok(cfg) => cfg,
        Err(msg) => {
            eprintln!("{msg}");
            std::process::exit(2);
        }
    };

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build tokio runtime");

    if cfg.warmup > 0 {
        let warmup_cap = effective_cap(cfg, cfg.warmup);
        rt.block_on(run_once(cfg.warmup, warmup_cap));
    }

    let cap = effective_cap(cfg, cfg.msgs);
    let elapsed = rt.block_on(run_once(cfg.msgs, cap));
    println!("bench=spsc_async_to_sync_actor");
    println!("msgs={}", cfg.msgs);
    println!("warmup={}", cfg.warmup);
    println!("cap_requested={}", cfg.cap);
    println!("cap={}", cap);
    println!("elapsed_s={:.6}", elapsed.as_secs_f64());
    println!("throughput_ops_s={:.3}", cfg.msgs as f64 / elapsed.as_secs_f64());
    println!("ns_per_op={:.3}", elapsed.as_nanos() as f64 / cfg.msgs as f64);
}
