mod _bench_util;

use _bench_util::{alloc_reset, alloc_snapshot, cpu_time, fmt_duration, print_alloc_stats_row};
use actix::prelude::*;
use std::env;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::Instant;

#[derive(Clone, Copy)]
struct Config {
    topics: usize,
    subs_per_topic: usize,
    msgs: u64,
    warmup: u64,
    cap: usize,
    hot_topic: bool,
}

fn usage() -> &'static str {
    "Usage: cargo bench --bench pubsub_local_actix -- [options]\n\
     \n\
     Options:\n\
       --topics N          Number of topics (default: 64)\n\
       --subs-per-topic N  Subscribers per topic (default: 256)\n\
       --msgs N            Number of publish operations (default: 200_000)\n\
       --warmup N          Warmup publishes (default: 20_000)\n\
       --cap N             Subscriber mailbox capacity (default: 65_536)\n\
       --hot-topic         Publish all messages to topic[0] (default: uniform routing)\n"
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
        hot_topic: false,
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
            "--hot-topic" => cfg.hot_topic = true,
            other => return Err(format!("unknown arg: {other}\n\n{}", usage())),
        }
    }

    Ok(cfg)
}

#[derive(Message)]
#[rtype(result = "()")]
struct FanoutMsg;

struct Subscriber {
    cap: usize,
    delivered: Arc<AtomicU64>,
}

impl Actor for Subscriber {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.set_mailbox_capacity(self.cap);
    }
}

impl Handler<FanoutMsg> for Subscriber {
    type Result = ();

    fn handle(&mut self, _msg: FanoutMsg, _ctx: &mut Self::Context) -> Self::Result {
        self.delivered.fetch_add(1, Ordering::Relaxed);
    }
}

fn expected_deliveries(msgs: u64, subs_per_topic: usize) -> u64 {
    msgs.saturating_mul(subs_per_topic as u64)
}

async fn wait_deliveries(target: u64, delivered: &AtomicU64) {
    while delivered.load(Ordering::Acquire) < target {
        tokio::task::yield_now().await;
    }
}

fn topic_idx(seq: u64, topics: usize, hot_topic: bool) -> usize {
    if hot_topic {
        0
    } else {
        (seq as usize) % topics
    }
}

fn publish_round(
    subs_by_topic: &[Vec<Addr<Subscriber>>],
    topics: usize,
    msgs: u64,
    hot_topic: bool,
    attempted: &AtomicU64,
    delivered: &AtomicU64,
    full: &AtomicU64,
) {
    for i in 0..msgs {
        let idx = topic_idx(i, topics, hot_topic);
        for addr in &subs_by_topic[idx] {
            attempted.fetch_add(1, Ordering::Relaxed);
            match addr.try_send(FanoutMsg) {
                Ok(()) => {
                    delivered.fetch_add(1, Ordering::Relaxed);
                }
                Err(SendError::Full(_)) => {
                    full.fetch_add(1, Ordering::Relaxed);
                }
                Err(SendError::Closed(_)) => {
                    panic!("actix subscriber closed");
                }
            }
        }
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

    let delivered_counter = Arc::new(AtomicU64::new(0));
    let attempted = Arc::new(AtomicU64::new(0));
    let delivered = Arc::new(AtomicU64::new(0));
    let full = Arc::new(AtomicU64::new(0));

    let cpu0 = cpu_time();
    let t0 = Instant::now();

    System::new().block_on(async {
        let mut subs_by_topic: Vec<Vec<Addr<Subscriber>>> =
            Vec::with_capacity(cfg.topics.saturating_mul(cfg.subs_per_topic));
        for _ in 0..cfg.topics {
            let mut topic = Vec::with_capacity(cfg.subs_per_topic);
            for _ in 0..cfg.subs_per_topic {
                topic.push(
                    Subscriber {
                        cap: cfg.cap,
                        delivered: Arc::clone(&delivered_counter),
                    }
                    .start(),
                );
            }
            subs_by_topic.push(topic);
        }

        publish_round(
            &subs_by_topic,
            cfg.topics,
            cfg.warmup,
            cfg.hot_topic,
            &attempted,
            &delivered,
            &full,
        );
        wait_deliveries(
            expected_deliveries(cfg.warmup, cfg.subs_per_topic),
            &delivered_counter,
        )
        .await;

        delivered_counter.store(0, Ordering::Relaxed);
        attempted.store(0, Ordering::Relaxed);
        delivered.store(0, Ordering::Relaxed);
        full.store(0, Ordering::Relaxed);
        alloc_reset();

        publish_round(
            &subs_by_topic,
            cfg.topics,
            cfg.msgs,
            cfg.hot_topic,
            &attempted,
            &delivered,
            &full,
        );
        wait_deliveries(expected_deliveries(cfg.msgs, cfg.subs_per_topic), &delivered_counter)
            .await;

        System::current().stop();
    });

    let wall = t0.elapsed();
    let cpu = cpu_time().saturating_sub(cpu0);
    let wall_s = wall.as_secs_f64();
    let cpu_s = cpu.as_secs_f64();
    let attempted_v = attempted.load(Ordering::Relaxed);
    let delivered_v = delivered.load(Ordering::Relaxed);
    let full_v = full.load(Ordering::Relaxed);

    println!("== actix pubsub local ==");
    println!(
        "topics: {}  subs_per_topic: {}  msgs: {}  warmup: {}",
        cfg.topics, cfg.subs_per_topic, cfg.msgs, cfg.warmup
    );
    println!("cap: {}  hot_topic: {}", cfg.cap, cfg.hot_topic);
    println!("stats: attempted={} delivered={} full={}", attempted_v, delivered_v, full_v);
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
            delivered_v as f64 / wall_s
        } else {
            0.0
        }
    );

    let alloc = alloc_snapshot();
    print_alloc_stats_row("pubsub_local_actix", cfg.msgs, &alloc);
}
