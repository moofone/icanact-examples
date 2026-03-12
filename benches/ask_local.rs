mod _bench_util;

use _bench_util::{
    LatencyBuf, decode_sample_t0, encode_sample_t0, fmt_duration, latency_stats_from_nanos, now_ns,
    print_latency_row, spin_then_yield,
};
use icanact_core::local_sync as sync;
use std::{collections::VecDeque, env, thread, time::Duration};

#[derive(Clone, Copy, Debug)]
struct Config {
    asks: u64,
    warmup: u64,
    cap: usize,
    sample_every: u64,
    inflight: usize,
    timeout_ms: u64,
}

#[derive(Debug)]
struct RunStats {
    wall: Duration,
    cpu: Duration,
    timeout_errors: u64,
    cancel_errors: u64,
    latency: _bench_util::LatencyStats,
    alloc: _bench_util::AllocStats,
}

fn usage() -> &'static str {
    "Usage: cargo bench -p icanact-core --bench ask_local -- [options]\n\
     \n\
     Options:\n\
       --asks N          Number of ask round-trips per variant (default: 1_000_000)\n\
       --warmup N        Warmup ask round-trips per variant (default: 100_000)\n\
       --cap N           Mailbox capacity (default: 65_536)\n\
       --sample-every N  Sample every Nth ask for RTT histogram (default: 8)\n\
       --inflight N      Max deferred asks in flight (default: 64)\n\
       --timeout-ms N    Timeout used by timeout variant (default: 5000)\n"
}

fn parse_u64(s: &str) -> Result<u64, String> {
    s.parse::<u64>().map_err(|e| e.to_string())
}

fn parse_usize(s: &str) -> Result<usize, String> {
    s.parse::<usize>().map_err(|e| e.to_string())
}

fn parse_config() -> Result<Config, String> {
    let mut cfg = Config {
        asks: 1_000_000,
        warmup: 100_000,
        cap: 65_536,
        sample_every: 8,
        inflight: 64,
        timeout_ms: 5_000,
    };

    let mut it = env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            // Cargo may pass libtest flags even for `harness = false` benches.
            "--bench" => {}
            "--help" | "-h" => return Err(usage().to_string()),
            "--asks" => {
                let v = it
                    .next()
                    .ok_or_else(|| "missing value for --asks".to_string())
                    .and_then(|s| parse_u64(&s))?;
                cfg.asks = v;
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
            "--sample-every" => {
                let v = it
                    .next()
                    .ok_or_else(|| "missing value for --sample-every".to_string())
                    .and_then(|s| parse_u64(&s))?;
                cfg.sample_every = v.max(1);
            }
            "--inflight" => {
                let v = it
                    .next()
                    .ok_or_else(|| "missing value for --inflight".to_string())
                    .and_then(|s| parse_usize(&s))?;
                cfg.inflight = v.max(1);
            }
            "--timeout-ms" => {
                let v = it
                    .next()
                    .ok_or_else(|| "missing value for --timeout-ms".to_string())
                    .and_then(|s| parse_u64(&s))?;
                cfg.timeout_ms = v.max(1);
            }
            other => return Err(format!("unknown arg: {other}\n\n{}", usage())),
        }
    }

    Ok(cfg)
}

enum Msg {
    Echo { v: u64, reply: sync::ReplyTo<u64> },
}

struct EchoActor;

impl icanact_core::Actor for EchoActor {
    type Msg = Msg;

    #[inline(always)]
    fn handle_tell(&mut self, msg: Self::Msg) {
        match msg {
            Msg::Echo { v, reply } => {
                let _ = reply.reply(v);
            }
        }
    }
}

fn run_deferred(cfg: Config, with_timeout: bool) -> RunStats {
    let samples_cap = (cfg.asks / cfg.sample_every).max(1) as usize + 64;
    let rec = LatencyBuf::new(samples_cap);
    let (addr, handle) = sync::spawn_actor(cfg.cap, EchoActor);
    handle.wait_for_startup();

    // Warmup with blocking ask to keep both variants comparable.
    for _ in 0..cfg.warmup {
        let _ = sync::ask(&addr, |reply| Msg::Echo { v: 0, reply }).expect("warmup ask");
    }

    _bench_util::alloc_reset();

    let mut pending: VecDeque<sync::PendingAsk<u64>> = VecDeque::with_capacity(cfg.inflight + 8);
    let cpu0 = _bench_util::cpu_time();
    let wall0 = std::time::Instant::now();
    let base = wall0;

    let mut timeout_errors = 0u64;
    let mut cancel_errors = 0u64;

    for i in 0..cfg.asks {
        let req = if i % cfg.sample_every == 0 {
            encode_sample_t0(now_ns(base))
        } else {
            0
        };

        let mut spins = 0u32;
        let p = loop {
            match sync::ask_deferred(&addr, |reply| Msg::Echo { v: req, reply }) {
                Ok(p) => break p,
                Err(sync::AskError::Full) => spin_then_yield(&mut spins),
                Err(sync::AskError::Canceled) => spin_then_yield(&mut spins),
                Err(sync::AskError::Timeout) => unreachable!("ask_deferred never returns Timeout"),
            }
        };
        pending.push_back(p);

        if pending.len() >= cfg.inflight {
            let p = pending.pop_front().expect("pending ask exists");
            let r = if with_timeout {
                p.wait_timeout(Duration::from_millis(cfg.timeout_ms))
            } else {
                p.wait()
            };

            match r {
                Ok(reply) => {
                    if let Some(t0) = decode_sample_t0(reply) {
                        rec.record(now_ns(base).saturating_sub(t0));
                    }
                }
                Err(sync::AskError::Timeout) => timeout_errors += 1,
                Err(sync::AskError::Canceled) => cancel_errors += 1,
                Err(sync::AskError::Full) => unreachable!("wait/wait_timeout does not return Full"),
            }
        }
    }

    while let Some(p) = pending.pop_front() {
        let r = if with_timeout {
            p.wait_timeout(Duration::from_millis(cfg.timeout_ms))
        } else {
            p.wait()
        };

        match r {
            Ok(reply) => {
                if let Some(t0) = decode_sample_t0(reply) {
                    rec.record(now_ns(base).saturating_sub(t0));
                }
            }
            Err(sync::AskError::Timeout) => timeout_errors += 1,
            Err(sync::AskError::Canceled) => cancel_errors += 1,
            Err(sync::AskError::Full) => unreachable!("wait/wait_timeout does not return Full"),
        }
    }

    let wall = wall0.elapsed();
    let cpu = _bench_util::cpu_time().saturating_sub(cpu0);
    let alloc = _bench_util::alloc_snapshot();

    handle.shutdown();
    thread::yield_now();

    RunStats {
        wall,
        cpu,
        timeout_errors,
        cancel_errors,
        latency: latency_stats_from_nanos(rec.collect()),
        alloc,
    }
}

fn print_variant(label: &str, cfg: Config, s: &RunStats) {
    let wall_s = s.wall.as_secs_f64();
    let cpu_s = s.cpu.as_secs_f64();
    let throughput = if wall_s > 0.0 {
        cfg.asks as f64 / wall_s
    } else {
        0.0
    };

    println!("== {label} ==");
    println!(
        "asks: {}  warmup: {}  cap: {}  sample_every: {}  inflight: {}  runtime: pooled",
        cfg.asks, cfg.warmup, cfg.cap, cfg.sample_every, cfg.inflight
    );
    println!(
        "wall: {}  cpu: {}  cpu_util: {:.2}",
        fmt_duration(s.wall),
        fmt_duration(s.cpu),
        if wall_s > 0.0 { cpu_s / wall_s } else { 0.0 }
    );
    println!("throughput: {:.3} ask/s", throughput);
    println!(
        "errors: timeout={} canceled={}",
        s.timeout_errors, s.cancel_errors
    );
    print_latency_row("ask_rtt", &s.latency);
    _bench_util::print_alloc_stats_row("ask_local", cfg.asks, &s.alloc);
}

fn main() {
    let cfg = match parse_config() {
        Ok(cfg) => cfg,
        Err(msg) => {
            eprintln!("{msg}");
            return;
        }
    };

    #[cfg(feature = "perf-counters")]
    sync::counters_reset();

    println!("== icanact-core local deferred ask ==");
    println!(
        "compare timeout vs no-timeout with identical in-flight window (timeout_ms={})",
        cfg.timeout_ms
    );
    println!();

    let no_timeout = run_deferred(cfg, false);
    print_variant("deferred ask (no-timeout wait)", cfg, &no_timeout);

    let with_timeout = run_deferred(cfg, true);
    print_variant("deferred ask (wait_timeout)", cfg, &with_timeout);

    let nt = no_timeout.wall.as_secs_f64();
    let wt = with_timeout.wall.as_secs_f64();
    let nt_tput = if nt > 0.0 { cfg.asks as f64 / nt } else { 0.0 };
    let wt_tput = if wt > 0.0 { cfg.asks as f64 / wt } else { 0.0 };
    let delta = if nt_tput > 0.0 {
        ((wt_tput / nt_tput) - 1.0) * 100.0
    } else {
        0.0
    };

    println!("== summary ==");
    println!("no-timeout throughput: {:.3} ask/s", nt_tput);
    println!("wait_timeout throughput: {:.3} ask/s", wt_tput);
    println!("wait_timeout vs no-timeout delta: {:+.2}%", delta);

    #[cfg(feature = "perf-counters")]
    {
        let counters = sync::counters_snapshot();
        println!("perf_counters_enabled: {}", sync::COUNTERS_ENABLED);
        println!("perf_counters.mpsc: {:?}", counters.mpsc);
        println!("perf_counters.ask: {:?}", counters.ask);
    }
}
