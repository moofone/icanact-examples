use icanact_core::local_sync as sync;
use std::env;
use std::hint::spin_loop;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug)]
enum Backend {
    Current,
    V2Kanal,
}

impl Backend {
    fn parse(raw: &str) -> Result<Self, String> {
        match raw {
            "current" => Ok(Self::Current),
            "v2-kanal" => Ok(Self::V2Kanal),
            other => Err(format!(
                "invalid --backend '{other}', expected current|v2-kanal"
            )),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Current => "current",
            Self::V2Kanal => "v2-kanal",
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct Config {
    backend: Backend,
    producers: usize,
    iters: u64,
    ops: u64,
    cap: usize,
}

struct BenchActor {
    processed: Arc<AtomicU64>,
}

impl icanact_core::runtime_contract::RuntimeContractMarker for BenchActor {
    type Contract = icanact_core::runtime_contract::SyncRuntime;
}

impl sync::SyncActor for BenchActor {}

impl icanact_core::Tell for BenchActor {
    type Msg = u64;

    #[inline(always)]
    fn handle_tell(&mut self, msg: icanact_core::ActorMessage<Self::Msg>) {
        std::hint::black_box(msg.payload);
        self.processed.fetch_add(1, Ordering::Relaxed);
    }
}

impl sync::SyncEndpoint for BenchActor {
    type RuntimeMsg = sync::SyncTellMessage<u64>;

    #[inline(always)]
    fn handle_runtime(&mut self, msg: icanact_core::ActorMessage<Self::RuntimeMsg>) {
        match msg.payload {
            sync::SyncTellMessage::Tell(v) => {
                std::hint::black_box(v);
                self.processed.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

impl sync::SyncTellEndpoint for BenchActor {
    #[inline(always)]
    fn runtime_tell(msg: <Self as icanact_core::Tell>::Msg) -> Self::RuntimeMsg {
        sync::SyncTellMessage::Tell(msg)
    }

    #[inline(always)]
    fn recover_tell(msg: Self::RuntimeMsg) -> Option<<Self as icanact_core::Tell>::Msg> {
        match msg {
            sync::SyncTellMessage::Tell(inner) => Some(inner),
        }
    }
}

fn usage() -> &'static str {
    "Usage: cargo bench --bench sync_runtime_tell_v2_compare -- [options]\n\
     \n\
     Environment overrides:\n\
       AF_EX_SYNC_BACKEND    current|v2-kanal\n\
       AF_EX_SYNC_PRODUCERS  Producer threads (default: 1)\n\
       AF_EX_SYNC_ITERS      Outer iterations (default: 10)\n\
       AF_EX_SYNC_OPS        Messages per iteration (default: 1000000)\n\
       AF_EX_SYNC_CAP        Mailbox capacity (default: 300000)\n\
     \n\
     CLI options:\n\
       --backend current|v2-kanal\n\
       --producers N\n\
       --iters N\n\
       --ops N\n\
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
        backend: Backend::parse(
            &env::var("AF_EX_SYNC_BACKEND").unwrap_or_else(|_| "current".into()),
        )?,
        producers: parse_usize_env("AF_EX_SYNC_PRODUCERS", 1).max(1),
        iters: parse_u64_env("AF_EX_SYNC_ITERS", 10).max(1),
        ops: parse_u64_env("AF_EX_SYNC_OPS", 1_000_000).max(1),
        cap: parse_usize_env("AF_EX_SYNC_CAP", 300_000).max(1),
    };
    let mut it = env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--help" | "-h" => return Err(usage().to_string()),
            "--bench" => {}
            "--backend" => {
                cfg.backend = Backend::parse(
                    &it.next()
                        .ok_or_else(|| "missing value for --backend".to_string())?,
                )?;
            }
            "--producers" => {
                cfg.producers = parse_usize(
                    &it.next()
                        .ok_or_else(|| "missing value for --producers".to_string())?,
                    "--producers",
                )?
                .max(1);
            }
            "--iters" => {
                cfg.iters = parse_u64(
                    &it.next()
                        .ok_or_else(|| "missing value for --iters".to_string())?,
                    "--iters",
                )?
                .max(1);
            }
            "--ops" => {
                cfg.ops = parse_u64(
                    &it.next()
                        .ok_or_else(|| "missing value for --ops".to_string())?,
                    "--ops",
                )?
                .max(1);
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

#[inline(always)]
fn spin_then_yield(spins: &mut u32) {
    if *spins < 64 {
        *spins += 1;
        spin_loop();
    } else {
        thread::yield_now();
    }
}

fn wait_until(processed: &AtomicU64, expected: u64) {
    let mut spins = 0u32;
    while processed.load(Ordering::Relaxed) < expected {
        spin_then_yield(&mut spins);
    }
}

fn messages_per_producer(total: u64, producers: usize, idx: usize) -> u64 {
    let base = total / producers as u64;
    let rem = total % producers as u64;
    base + u64::from((idx as u64) < rem)
}

fn run_current(cfg: Config) -> Duration {
    let processed = Arc::new(AtomicU64::new(0));
    let (addr, handle) = sync::spawn_runtime_actor_with_opts(
        BenchActor {
            processed: Arc::clone(&processed),
        },
        sync::SpawnOpts {
            mailbox_capacity: cfg.cap,
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
    wait_until(&processed, warmup);

    let start = Instant::now();
    for _ in 0..cfg.iters {
        processed.store(0, Ordering::Relaxed);
        thread::scope(|scope| {
            for producer_idx in 0..cfg.producers {
                let addr_cloned = addr.clone();
                let count = messages_per_producer(cfg.ops, cfg.producers, producer_idx);
                scope.spawn(move || {
                    for _ in 0..count {
                        let mut spins = 0u32;
                        while !addr_cloned.tell(1) {
                            spin_then_yield(&mut spins);
                        }
                    }
                });
            }
        });
        wait_until(&processed, cfg.ops);
    }
    let elapsed = start.elapsed();
    handle.shutdown();
    elapsed
}

fn run_v2_kanal(cfg: Config) -> Duration {
    let processed = Arc::new(AtomicU64::new(0));
    let processed_worker = Arc::clone(&processed);
    let (addr, handle) =
        sync::mailbox_v2::spawn(cfg.cap, move |msg: sync::SyncTellMessage<u64>| match msg {
            sync::SyncTellMessage::Tell(v) => {
                std::hint::black_box(v);
                processed_worker.fetch_add(1, Ordering::Relaxed);
            }
        });

    let warmup = cfg.ops.min(10_000);
    for _ in 0..warmup {
        addr.tell(sync::SyncTellMessage::Tell(1)).unwrap();
    }
    wait_until(&processed, warmup);

    let start = Instant::now();
    for _ in 0..cfg.iters {
        processed.store(0, Ordering::Relaxed);
        thread::scope(|scope| {
            for producer_idx in 0..cfg.producers {
                let addr_cloned = addr.clone();
                let count = messages_per_producer(cfg.ops, cfg.producers, producer_idx);
                scope.spawn(move || {
                    for _ in 0..count {
                        addr_cloned.tell(sync::SyncTellMessage::Tell(1)).unwrap();
                    }
                });
            }
        });
        wait_until(&processed, cfg.ops);
    }
    let elapsed = start.elapsed();
    handle.shutdown();
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

    let elapsed = match cfg.backend {
        Backend::Current => run_current(cfg),
        Backend::V2Kanal => run_v2_kanal(cfg),
    };
    let total = cfg.iters.saturating_mul(cfg.ops);
    let secs = elapsed.as_secs_f64();
    let ops_per_sec = if secs > 0.0 { total as f64 / secs } else { 0.0 };

    println!("bench=sync_runtime_tell_v2_compare");
    println!("backend={}", cfg.backend.as_str());
    println!("producers={}", cfg.producers);
    println!("iters={}", cfg.iters);
    println!("ops={}", cfg.ops);
    println!("cap={}", cfg.cap);
    println!("elapsed_s={secs:.6}");
    println!("ops_per_sec={ops_per_sec:.3}");
}
