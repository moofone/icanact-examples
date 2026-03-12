use icanact_core::{Actor, local_sync as sync};
use std::{
    env,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

#[derive(Clone, Copy, Debug)]
struct Config {
    iters: u64,
    warmup: u64,
    cap: usize,
    pool_size: usize,
    spin_init: usize,
}

fn usage() -> &'static str {
    "Usage: cargo bench -p icanact-core --bench spawn_pool -- [options]\n\
     \n\
     Options:\n\
       --iters N       Number of spawn cycles per variant (default: 200000)\n\
       --warmup N      Warmup spawn cycles per variant (default: 1000)\n\
       --cap N         Mailbox capacity (default: 64)\n\
       --pool-size N   Default pool size for sync::spawn_actor (default: available_parallelism)\n\
       --spin-init N   Spin iterations before parking while idle (default: 0)\n"
}

fn parse_u64(s: &str) -> Result<u64, String> {
    s.parse::<u64>().map_err(|e| e.to_string())
}

fn parse_usize(s: &str) -> Result<usize, String> {
    s.parse::<usize>().map_err(|e| e.to_string())
}

fn default_pool_size() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

fn parse_config() -> Result<Config, String> {
    let mut cfg = Config {
        iters: 200_000,
        warmup: 1_000,
        cap: 64,
        pool_size: default_pool_size(),
        spin_init: 64,
    };

    let mut it = env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            // Cargo may pass libtest flags even for `harness = false` benches.
            "--bench" => {}
            "--help" | "-h" => return Err(usage().to_string()),
            "--iters" => {
                let v = it
                    .next()
                    .ok_or_else(|| "missing value for --iters".to_string())
                    .and_then(|s| parse_u64(&s))?;
                cfg.iters = v;
            }
            "--warmup" => {
                let v = it
                    .next()
                    .ok_or_else(|| "missing value for --warmup".to_string())
                    .and_then(|s| parse_u64(&s))?;
                cfg.warmup = v;
            }
            "--cap" => {
                let v = it
                    .next()
                    .ok_or_else(|| "missing value for --cap".to_string())
                    .and_then(|s| parse_usize(&s))?;
                cfg.cap = v;
            }
            "--pool-size" => {
                let v = it
                    .next()
                    .ok_or_else(|| "missing value for --pool-size".to_string())
                    .and_then(|s| parse_usize(&s))?;
                if v == 0 {
                    return Err("--pool-size must be >= 1".to_string());
                }
                cfg.pool_size = v;
            }
            "--spin-init" => {
                let v = it
                    .next()
                    .ok_or_else(|| "missing value for --spin-init".to_string())
                    .and_then(|s| parse_usize(&s))?;
                cfg.spin_init = v;
            }
            other => return Err(format!("unknown arg: {other}\n\n{}", usage())),
        }
    }

    Ok(cfg)
}

fn cpu_time() -> Duration {
    unsafe {
        let mut usage: libc::rusage = std::mem::zeroed();
        libc::getrusage(libc::RUSAGE_SELF, &mut usage);
        let user = Duration::new(
            usage.ru_utime.tv_sec as u64,
            (usage.ru_utime.tv_usec as u32) * 1_000,
        );
        let sys = Duration::new(
            usage.ru_stime.tv_sec as u64,
            (usage.ru_stime.tv_usec as u32) * 1_000,
        );
        user + sys
    }
}

fn fmt_duration(d: Duration) -> String {
    let secs = d.as_secs_f64();
    if secs < 1e-6 {
        format!("{:.3}ns", secs * 1e9)
    } else if secs < 1e-3 {
        format!("{:.3}us", secs * 1e6)
    } else if secs < 1.0 {
        format!("{:.3}ms", secs * 1e3)
    } else {
        format!("{:.3}s", secs)
    }
}

#[derive(Clone)]
struct BenchActor {
    started_token: Arc<AtomicU64>,
    token: u64,
}

impl Actor for BenchActor {
    type Msg = ();

    #[inline(always)]
    fn on_start(&mut self) {
        self.started_token.store(self.token, Ordering::Release);
    }

    #[inline(always)]
    fn handle_tell(&mut self, _msg: Self::Msg) {}
}

fn bench_variant<FSpawn>(name: &str, cfg: Config, mut spawn: FSpawn)
where
    FSpawn: FnMut(BenchActor) -> sync::ActorHandle,
{
    let started = Arc::new(AtomicU64::new(0));

    // Warmup.
    for i in 1..=cfg.warmup {
        started.store(0, Ordering::Relaxed);
        let h = spawn(BenchActor {
            started_token: Arc::clone(&started),
            token: i,
        });
        while started.load(Ordering::Acquire) != i {
            std::hint::spin_loop();
        }
        h.shutdown();
    }

    let mut spawn_total = Duration::ZERO;
    let mut stop_total = Duration::ZERO;

    let cpu0 = cpu_time();
    let wall0 = Instant::now();

    for i in 1..=cfg.iters {
        let token = cfg.warmup + i;
        started.store(0, Ordering::Relaxed);

        let t0 = Instant::now();
        let h = spawn(BenchActor {
            started_token: Arc::clone(&started),
            token,
        });
        while started.load(Ordering::Acquire) != token {
            std::hint::spin_loop();
        }
        spawn_total += t0.elapsed();

        let t1 = Instant::now();
        h.shutdown();
        stop_total += t1.elapsed();
    }

    let wall = wall0.elapsed();
    let cpu = cpu_time().saturating_sub(cpu0);
    let wall_s = wall.as_secs_f64();
    let cpu_s = cpu.as_secs_f64();

    let spawn_avg_ns = spawn_total.as_secs_f64() * 1e9 / (cfg.iters as f64);
    let stop_avg_ns = stop_total.as_secs_f64() * 1e9 / (cfg.iters as f64);

    println!("== {name} ==");
    println!(
        "iters: {}  warmup: {}  cap: {}  pool_size: {}  spin_init: {}",
        cfg.iters, cfg.warmup, cfg.cap, cfg.pool_size, cfg.spin_init
    );
    println!("spawn_to_ready_total: {}", fmt_duration(spawn_total));
    println!("spawn_to_ready_avg: {:.1}ns", spawn_avg_ns);
    println!("stop_to_gone_total: {}", fmt_duration(stop_total));
    println!("stop_to_gone_avg: {:.1}ns", stop_avg_ns);
    println!(
        "process totals: wall={} cpu={} cpu_util={:.2}",
        fmt_duration(wall),
        fmt_duration(cpu),
        if wall_s > 0.0 { cpu_s / wall_s } else { 0.0 }
    );
    println!();
}

fn main() {
    let cfg = match parse_config() {
        Ok(cfg) => cfg,
        Err(msg) => {
            eprintln!("{msg}");
            return;
        }
    };

    // Configure the default pool *before* any spawns of this actor type.
    let _ = sync::set_default_pool_config(sync::PoolConfig {
        size: cfg.pool_size,
        spin_init: cfg.spin_init,
    });

    println!(
        "config: iters={} warmup={} cap={} pool_size={} spin_init={}",
        cfg.iters, cfg.warmup, cfg.cap, cfg.pool_size, cfg.spin_init
    );
    println!();

    bench_variant(
        "icanact-core spawn_to_ready (pooled: sync::spawn_actor)",
        cfg,
        |a| {
            let (_addr, h) = sync::spawn_actor(cfg.cap, a);
            h
        },
    );
}
