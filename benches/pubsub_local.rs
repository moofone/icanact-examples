mod _bench_util;

use _bench_util::{alloc_reset, alloc_snapshot, cpu_time, fmt_duration, print_alloc_stats_row};
use icanact_core::local_sync::{self as sync, BroadcastConfig, PubSub, PublishStats};
use std::env;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::thread;
use std::time::Instant;

#[derive(Clone, Copy)]
struct Config {
    topics: usize,
    subs_per_topic: usize,
    msgs: u64,
    warmup: u64,
    cap: usize,
    publishers: usize,
    hot_topic: bool,
    stripes: usize,
    threshold: usize,
}

fn usage() -> &'static str {
    "Usage: cargo bench -p icanact-core --bench pubsub_local -- [options]\n\
     \n\
     Options:\n\
       --topics N          Number of topics (default: 64)\n\
       --subs-per-topic N  Subscribers per topic (default: 256)\n\
       --msgs N            Number of publish operations (default: 200_000)\n\
       --warmup N          Warmup publishes (default: 20_000)\n\
       --cap N             Subscriber mailbox capacity (default: 65_536)\n\
       --publishers N      Publisher threads (default: 1)\n\
       --hot-topic         Publish all messages to topic[0] (default: uniform routing)\n\
       --stripes N         Fan-out worker stripes (default: available_parallelism)\n\
       --threshold N       Adaptive concurrent threshold (default: 128)\n"
}

fn parse_u64(s: &str) -> Result<u64, String> {
    s.parse::<u64>().map_err(|e| e.to_string())
}

fn parse_usize(s: &str) -> Result<usize, String> {
    s.parse::<usize>().map_err(|e| e.to_string())
}

fn parse_config() -> Result<Config, String> {
    let mut cfg = Config {
        topics: 64,
        subs_per_topic: 256,
        msgs: 200_000,
        warmup: 20_000,
        cap: 65_536,
        publishers: 1,
        hot_topic: false,
        stripes: std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1),
        threshold: 128,
    };

    let mut it = env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--bench" => {}
            "--help" | "-h" => return Err(usage().to_string()),
            "--topics" => {
                cfg.topics = it
                    .next()
                    .ok_or_else(|| "missing value for --topics".to_string())
                    .and_then(|s| parse_usize(&s))?
                    .max(1);
            }
            "--subs-per-topic" => {
                cfg.subs_per_topic = it
                    .next()
                    .ok_or_else(|| "missing value for --subs-per-topic".to_string())
                    .and_then(|s| parse_usize(&s))?
                    .max(1);
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
            "--hot-topic" => cfg.hot_topic = true,
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

fn publish_n(
    pubsub: &PubSub<u64>,
    topics: &[String],
    hot_topic: bool,
    start_seq: u64,
    n: u64,
) -> PublishStats {
    let mut stats = PublishStats::default();
    for i in 0..n {
        let idx = if hot_topic {
            0
        } else {
            ((start_seq + i) as usize) % topics.len()
        };
        let s = pubsub.publish(&topics[idx], 1);
        merge_stats(&mut stats, s);
    }
    stats
}

fn run_publishers(
    pubsub: PubSub<u64>,
    topics: Arc<Vec<String>>,
    cfg: Config,
    msgs: u64,
) -> PublishStats {
    if cfg.publishers <= 1 {
        return publish_n(&pubsub, topics.as_slice(), cfg.hot_topic, 0, msgs);
    }

    let p = cfg.publishers as u64;
    let base = msgs / p;
    let rem = msgs % p;
    let mut joins = Vec::with_capacity(cfg.publishers);
    for i in 0..cfg.publishers {
        let ps = pubsub.clone();
        let topics2 = Arc::clone(&topics);
        let mut n = base;
        if (i as u64) < rem {
            n += 1;
        }
        let start = (i as u64).saturating_mul(base);
        joins.push(thread::spawn(move || {
            publish_n(&ps, topics2.as_slice(), cfg.hot_topic, start, n)
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

fn expected_deliveries(msgs: u64, subs_per_topic: usize) -> u64 {
    msgs.saturating_mul(subs_per_topic as u64)
}

fn main() {
    let cfg = match parse_config() {
        Ok(cfg) => cfg,
        Err(msg) => {
            eprintln!("{msg}");
            return;
        }
    };

    let pubsub = PubSub::<u64>::with_broadcast_config(BroadcastConfig {
        stripes: cfg.stripes,
        concurrent_threshold: cfg.threshold,
    });

    let topics: Vec<String> = (0..cfg.topics)
        .map(|i| format!("af/bench/topic/{i}/v1"))
        .collect();
    let topics = Arc::new(topics);

    let delivered = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::with_capacity(cfg.topics.saturating_mul(cfg.subs_per_topic));
    for topic in topics.iter() {
        for _ in 0..cfg.subs_per_topic {
            let delivered2 = Arc::clone(&delivered);
            let (addr, h) = sync::mpsc::spawn(cfg.cap, move |_m: u64| {
                delivered2.fetch_add(1, Ordering::Relaxed);
            });
            let _ = pubsub.subscribe(topic, addr);
            handles.push(h);
        }
    }

    let warmup_stats = run_publishers(pubsub.clone(), Arc::clone(&topics), cfg, cfg.warmup);
    wait_deliveries(
        expected_deliveries(cfg.warmup, cfg.subs_per_topic),
        &delivered,
    );
    delivered.store(0, Ordering::Relaxed);
    alloc_reset();

    let cpu0 = cpu_time();
    let t0 = Instant::now();
    let stats = run_publishers(pubsub.clone(), Arc::clone(&topics), cfg, cfg.msgs);
    wait_deliveries(
        expected_deliveries(cfg.msgs, cfg.subs_per_topic),
        &delivered,
    );
    let wall = t0.elapsed();
    let cpu = cpu_time().saturating_sub(cpu0);

    for h in handles {
        h.shutdown();
    }

    let wall_s = wall.as_secs_f64();
    let cpu_s = cpu.as_secs_f64();

    println!("== icanact-core pubsub local ==");
    println!(
        "topics: {}  subs_per_topic: {}  msgs: {}  warmup: {}",
        cfg.topics, cfg.subs_per_topic, cfg.msgs, cfg.warmup
    );
    println!(
        "publishers: {}  cap: {}  hot_topic: {}",
        cfg.publishers, cfg.cap, cfg.hot_topic
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
    print_alloc_stats_row("pubsub_local", cfg.msgs, &alloc);
}
