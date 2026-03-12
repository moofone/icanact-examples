mod _bench_util;

use _bench_util::{alloc_reset, alloc_snapshot, cpu_time, fmt_duration, print_alloc_stats_row};
use icanact_core::local_sync::{self as sync, BroadcastConfig, BroadcastGroup, PublishStats};
use std::env;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::thread;
use std::time::Instant;

#[derive(Clone, Copy)]
enum Mode {
    Seq,
    Concurrent,
    Adaptive,
}

#[derive(Clone, Copy)]
struct Config {
    subs: usize,
    msgs: u64,
    warmup: u64,
    cap: usize,
    publishers: usize,
    stripes: usize,
    threshold: usize,
    mode: Mode,
}

fn usage() -> &'static str {
    "Usage: cargo bench -p icanact-core --bench broadcast_local -- [options]\n\
     \n\
     Options:\n\
       --subs N         Number of subscribers (default: 8192)\n\
       --msgs N         Number of publish operations (default: 200_000)\n\
       --warmup N       Warmup publishes (default: 20_000)\n\
       --cap N          Subscriber mailbox capacity (default: 65_536)\n\
       --publishers N   Publisher threads (default: 1)\n\
       --stripes N      Fan-out worker stripes (default: available_parallelism)\n\
       --threshold N    Adaptive concurrent threshold (default: 128)\n\
       --mode MODE      seq|concurrent|adaptive (default: adaptive)\n"
}

fn parse_u64(s: &str) -> Result<u64, String> {
    s.parse::<u64>().map_err(|e| e.to_string())
}

fn parse_usize(s: &str) -> Result<usize, String> {
    s.parse::<usize>().map_err(|e| e.to_string())
}

fn parse_mode(s: &str) -> Result<Mode, String> {
    match s {
        "seq" => Ok(Mode::Seq),
        "concurrent" => Ok(Mode::Concurrent),
        "adaptive" => Ok(Mode::Adaptive),
        other => Err(format!("unknown mode: {other}")),
    }
}

fn parse_config() -> Result<Config, String> {
    let mut cfg = Config {
        subs: 8192,
        msgs: 200_000,
        warmup: 20_000,
        cap: 65_536,
        publishers: 1,
        stripes: std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1),
        threshold: 128,
        mode: Mode::Adaptive,
    };

    let mut it = env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--bench" => {}
            "--help" | "-h" => return Err(usage().to_string()),
            "--subs" => {
                cfg.subs = it
                    .next()
                    .ok_or_else(|| "missing value for --subs".to_string())
                    .and_then(|s| parse_usize(&s))?;
            }
            "--msgs" => {
                cfg.msgs = it
                    .next()
                    .ok_or_else(|| "missing value for --msgs".to_string())
                    .and_then(|s| parse_u64(&s))?;
            }
            "--warmup" => {
                cfg.warmup = it
                    .next()
                    .ok_or_else(|| "missing value for --warmup".to_string())
                    .and_then(|s| parse_u64(&s))?;
            }
            "--cap" => {
                cfg.cap = it
                    .next()
                    .ok_or_else(|| "missing value for --cap".to_string())
                    .and_then(|s| parse_usize(&s))?;
            }
            "--publishers" => {
                cfg.publishers = it
                    .next()
                    .ok_or_else(|| "missing value for --publishers".to_string())
                    .and_then(|s| parse_usize(&s))?
                    .max(1);
            }
            "--stripes" => {
                cfg.stripes = it
                    .next()
                    .ok_or_else(|| "missing value for --stripes".to_string())
                    .and_then(|s| parse_usize(&s))?
                    .max(1);
            }
            "--threshold" => {
                cfg.threshold = it
                    .next()
                    .ok_or_else(|| "missing value for --threshold".to_string())
                    .and_then(|s| parse_usize(&s))?
                    .max(1);
            }
            "--mode" => {
                cfg.mode = it
                    .next()
                    .ok_or_else(|| "missing value for --mode".to_string())
                    .and_then(|s| parse_mode(&s))?;
            }
            other => return Err(format!("unknown arg: {other}\n\n{}", usage())),
        }
    }

    Ok(cfg)
}

fn merge_stats(dst: &mut PublishStats, src: PublishStats) {
    dst.attempted = dst.attempted.saturating_add(src.attempted);
    dst.delivered = dst.delivered.saturating_add(src.delivered);
    dst.full = dst.full.saturating_add(src.full);
}

fn publish_once(group: &BroadcastGroup<u64>, mode: Mode) -> PublishStats {
    match mode {
        Mode::Seq => group.publish_seq(1),
        Mode::Concurrent => group.publish_concurrent(1),
        Mode::Adaptive => group.publish(1),
    }
}

fn run_publishers(group: Arc<BroadcastGroup<u64>>, cfg: Config, msgs: u64) -> PublishStats {
    if cfg.publishers <= 1 {
        let mut acc = PublishStats::default();
        for _ in 0..msgs {
            merge_stats(&mut acc, publish_once(&group, cfg.mode));
        }
        return acc;
    }

    let p = cfg.publishers as u64;
    let base = msgs / p;
    let rem = msgs % p;
    let mut joins = Vec::with_capacity(cfg.publishers);

    for i in 0..cfg.publishers {
        let g = Arc::clone(&group);
        let mode = cfg.mode;
        let mut n = base;
        if (i as u64) < rem {
            n += 1;
        }
        joins.push(thread::spawn(move || {
            let mut acc = PublishStats::default();
            for _ in 0..n {
                merge_stats(&mut acc, publish_once(&g, mode));
            }
            acc
        }));
    }

    let mut acc = PublishStats::default();
    for join in joins {
        merge_stats(&mut acc, join.join().expect("publisher thread panicked"));
    }
    acc
}

fn wait_deliveries(target: u64, delivered: &AtomicU64) {
    while delivered.load(Ordering::Acquire) < target {
        thread::yield_now();
    }
}

fn mode_name(mode: Mode) -> &'static str {
    match mode {
        Mode::Seq => "seq",
        Mode::Concurrent => "concurrent",
        Mode::Adaptive => "adaptive",
    }
}

fn main() {
    let cfg = match parse_config() {
        Ok(cfg) => cfg,
        Err(msg) => {
            eprintln!("{msg}");
            return;
        }
    };

    let group = Arc::new(BroadcastGroup::with_config(BroadcastConfig {
        stripes: cfg.stripes,
        concurrent_threshold: cfg.threshold,
    }));

    let delivered = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::with_capacity(cfg.subs);
    for _ in 0..cfg.subs {
        let delivered2 = Arc::clone(&delivered);
        let (addr, h) = sync::mpsc::spawn(cfg.cap, move |_m: u64| {
            delivered2.fetch_add(1, Ordering::Relaxed);
        });
        let _ = group.subscribe(addr);
        handles.push(h);
    }

    let warmup_stats = run_publishers(Arc::clone(&group), cfg, cfg.warmup);
    wait_deliveries(cfg.warmup.saturating_mul(cfg.subs as u64), &delivered);
    delivered.store(0, Ordering::Relaxed);
    alloc_reset();

    let cpu0 = cpu_time();
    let t0 = Instant::now();
    let stats = run_publishers(Arc::clone(&group), cfg, cfg.msgs);
    wait_deliveries(cfg.msgs.saturating_mul(cfg.subs as u64), &delivered);
    let wall = t0.elapsed();
    let cpu = cpu_time().saturating_sub(cpu0);

    for h in handles {
        h.shutdown();
    }

    let wall_s = wall.as_secs_f64();
    let cpu_s = cpu.as_secs_f64();
    println!("== icanact-core broadcast local ==");
    println!(
        "mode: {}  subs: {}  msgs: {}  warmup: {}  publishers: {}  cap: {}",
        mode_name(cfg.mode),
        cfg.subs,
        cfg.msgs,
        cfg.warmup,
        cfg.publishers,
        cfg.cap
    );
    println!("stripes: {}  threshold: {}", cfg.stripes, cfg.threshold);
    println!(
        "warmup_stats: attempted={} delivered={} full={}",
        warmup_stats.attempted, warmup_stats.delivered, warmup_stats.full
    );
    println!(
        "stats: attempted={} delivered={} full={}",
        stats.attempted, stats.delivered, stats.full
    );
    println!(
        "wall: {}  cpu: {}  cpu_util: {:.2}",
        fmt_duration(wall),
        fmt_duration(cpu),
        if wall_s > 0.0 { cpu_s / wall_s } else { 0.0 }
    );
    println!(
        "publish_ops_per_s: {:.3}",
        if wall_s > 0.0 {
            cfg.msgs as f64 / wall_s
        } else {
            0.0
        }
    );
    println!(
        "deliveries_per_s: {:.3}",
        if wall_s > 0.0 {
            stats.delivered as f64 / wall_s
        } else {
            0.0
        }
    );

    let alloc = alloc_snapshot();
    print_alloc_stats_row("broadcast_local", cfg.msgs, &alloc);
}
