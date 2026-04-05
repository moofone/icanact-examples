mod _bench_util;

use _bench_util::{alloc_reset, alloc_snapshot, cpu_time, fmt_duration, print_alloc_stats_row};
use bytes::Bytes;
use icanact_core::local_async as af_async;
use serde::Deserialize;
use std::{
    env,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

#[derive(Clone, Copy, Debug)]
struct Config {
    msgs: u64,
    warmup: u64,
    cap: usize,
    yield_every: u64,
    mode: Mode,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    TypedSingleSameThread,
    TypedRoute2SameThread,
}

fn usage() -> &'static str {
    "Usage: cargo bench --bench json_same_thread_async -- [options]\n\
     \n\
     Options:\n\
       --msgs N          Measurement messages (default: 500_000)\n\
       --warmup N        Warmup messages (default: 50_000)\n\
       --cap N           Mailbox capacity (default: 1024)\n\
       --yield-every N   Producer yields every N sends (default: 256)\n\
       --mode MODE       typed_single_same_thread | typed_route_2_same_thread (default: typed_single_same_thread)\n"
}

fn parse_u64(s: &str) -> Result<u64, String> {
    s.parse::<u64>().map_err(|e| e.to_string())
}

fn parse_usize(s: &str) -> Result<usize, String> {
    s.parse::<usize>().map_err(|e| e.to_string())
}

fn parse_mode(s: &str) -> Result<Mode, String> {
    match s {
        "typed_single_same_thread" => Ok(Mode::TypedSingleSameThread),
        "typed_route_2_same_thread" => Ok(Mode::TypedRoute2SameThread),
        other => Err(format!("invalid --mode: {other}")),
    }
}

fn parse_config() -> Result<Config, String> {
    let mut cfg = Config {
        msgs: 500_000,
        warmup: 50_000,
        cap: 1024,
        yield_every: 256,
        mode: Mode::TypedSingleSameThread,
    };

    let mut it = env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--bench" => {}
            "--help" | "-h" => return Err(usage().to_string()),
            "--msgs" => cfg.msgs = parse_u64(&it.next().ok_or("missing --msgs")?)?,
            "--warmup" => cfg.warmup = parse_u64(&it.next().ok_or("missing --warmup")?)?,
            "--cap" => cfg.cap = parse_usize(&it.next().ok_or("missing --cap")?)?,
            "--yield-every" => {
                cfg.yield_every = parse_u64(&it.next().ok_or("missing --yield-every")?)?
            }
            "--mode" => cfg.mode = parse_mode(&it.next().ok_or("missing --mode")?)?,
            other => return Err(format!("unknown arg: {other}\n\n{}", usage())),
        }
    }

    if cfg.cap == 0 {
        return Err("--cap must be >= 1".to_string());
    }

    Ok(cfg)
}

#[derive(Clone, Copy, Debug)]
struct TradeLite {
    symbol: u16,
    ts_ms: u64,
    price: f64,
    qty: f64,
}

#[derive(Debug, Deserialize)]
struct TradeJson<'a> {
    #[serde(borrow)]
    sym: &'a str,
    ts: u64,
    price: f64,
    qty: f64,
}

#[inline(always)]
fn symbol_id(sym: &str) -> u16 {
    let b = sym.as_bytes();
    (b.get(b.len().saturating_sub(1)).copied().unwrap_or(b'0') - b'0') as u16
}

#[inline(always)]
fn parse_trade_from_json_bytes(payload: &[u8]) -> TradeLite {
    let v: TradeJson<'_> = sonic_rs::from_slice(payload).expect("valid trade json");
    TradeLite {
        symbol: symbol_id(v.sym),
        ts_ms: v.ts,
        price: v.price,
        qty: v.qty,
    }
}

fn build_payloads() -> Arc<Vec<Bytes>> {
    const PAYLOADS: usize = 128;
    let mut payloads = Vec::with_capacity(PAYLOADS);
    for idx in 0..PAYLOADS {
        let lane = idx & 1;
        let s = format!(
            "{{\"sym\":\"SYM{lane}\",\"ts\":1700000000000,\"price\":123.45,\"qty\":0.67}}"
        );
        payloads.push(Bytes::from(s));
    }
    Arc::new(payloads)
}

struct SinkActor {
    processed: Arc<AtomicU64>,
}

impl af_async::AsyncActor for SinkActor {}

impl af_async::Actor for SinkActor {
    type ActorMailboxContract = af_async::contract::TellOnly;
    type Tell = TradeLite;
    type Ask = af_async::contract::NoAsk;
    type Reply = af_async::contract::NoReply;

    async fn handle_tell(&mut self, msg: Self::Tell) {
        std::hint::black_box((msg.symbol, msg.ts_ms, msg.price, msg.qty));
        self.processed.fetch_add(1, Ordering::Relaxed);
    }
}

struct WsSingleActor {
    downstream: af_async::AsyncActorRef<SinkActor>,
}

impl af_async::AsyncActor for WsSingleActor {}

impl af_async::Actor for WsSingleActor {
    type ActorMailboxContract = af_async::contract::TellOnly;
    type Tell = Bytes;
    type Ask = af_async::contract::NoAsk;
    type Reply = af_async::contract::NoReply;

    async fn handle_tell(&mut self, msg: Self::Tell) {
        let trade = parse_trade_from_json_bytes(&msg);
        while !self.downstream.tell(trade) {
            tokio::task::yield_now().await;
        }
    }
}

struct WsRouteActor {
    downstream_a: af_async::AsyncActorRef<SinkActor>,
    downstream_b: af_async::AsyncActorRef<SinkActor>,
}

impl af_async::AsyncActor for WsRouteActor {}

impl af_async::Actor for WsRouteActor {
    type ActorMailboxContract = af_async::contract::TellOnly;
    type Tell = Bytes;
    type Ask = af_async::contract::NoAsk;
    type Reply = af_async::contract::NoReply;

    async fn handle_tell(&mut self, msg: Self::Tell) {
        let trade = parse_trade_from_json_bytes(&msg);
        let target = if (trade.symbol & 1) == 0 {
            &self.downstream_a
        } else {
            &self.downstream_b
        };
        while !target.tell(trade) {
            tokio::task::yield_now().await;
        }
    }
}

async fn send_payloads<A>(
    count: u64,
    yield_every: u64,
    payloads: &Arc<Vec<Bytes>>,
    addr: &af_async::AsyncActorRef<A>,
) where
    A: af_async::Actor<ActorMailboxContract = af_async::contract::TellOnly, Tell = Bytes>,
{
    for idx in 0..count {
        let payload = payloads[(idx as usize) % payloads.len()].clone();
        while !addr.tell(payload.clone()) {
            tokio::task::yield_now().await;
        }
        if yield_every > 0 && ((idx + 1) % yield_every == 0) {
            tokio::task::yield_now().await;
        }
    }
}

async fn run_single(cfg: Config) -> (std::time::Duration, std::time::Duration, u64) {
    let payloads = build_payloads();
    let processed = Arc::new(AtomicU64::new(0));

    let (sink_addr, sink_handle) = af_async::spawn_with_opts(
        SinkActor {
            processed: Arc::clone(&processed),
        },
        af_async::SpawnOpts {
            mailbox_capacity: cfg.cap,
            ..Default::default()
        },
    );
    let (ws_addr, ws_handle) = af_async::spawn_with_opts(
        WsSingleActor {
            downstream: sink_addr,
        },
        af_async::SpawnOpts {
            mailbox_capacity: cfg.cap,
            ..Default::default()
        },
    );

    send_payloads(cfg.warmup, cfg.yield_every, &payloads, &ws_addr).await;

    while processed.load(Ordering::Relaxed) < cfg.warmup {
        tokio::task::yield_now().await;
    }
    processed.store(0, Ordering::Relaxed);
    alloc_reset();
    let cpu0 = cpu_time();
    let t0 = Instant::now();

    for idx in 0..cfg.msgs {
        let payload = payloads[(idx as usize) % payloads.len()].clone();
        while !ws_addr.tell(payload.clone()) {
            tokio::task::yield_now().await;
        }
        if cfg.yield_every > 0 && ((idx + 1) % cfg.yield_every == 0) {
            tokio::task::yield_now().await;
        }
    }

    while processed.load(Ordering::Relaxed) < cfg.msgs {
        tokio::task::yield_now().await;
    }

    let wall = t0.elapsed();
    let cpu = cpu_time().saturating_sub(cpu0);
    let stats = alloc_snapshot();
    print_alloc_stats_row("typed_single_same_thread", cfg.msgs, &stats);

    ws_handle.shutdown().await;
    sink_handle.shutdown().await;
    (wall, cpu, cfg.msgs)
}

async fn run_route2(cfg: Config) -> (std::time::Duration, std::time::Duration, u64) {
    let payloads = build_payloads();
    let processed = Arc::new(AtomicU64::new(0));

    let (sink_a, sink_a_handle) = af_async::spawn_with_opts(
        SinkActor {
            processed: Arc::clone(&processed),
        },
        af_async::SpawnOpts {
            mailbox_capacity: cfg.cap,
            ..Default::default()
        },
    );
    let (sink_b, sink_b_handle) = af_async::spawn_with_opts(
        SinkActor {
            processed: Arc::clone(&processed),
        },
        af_async::SpawnOpts {
            mailbox_capacity: cfg.cap,
            ..Default::default()
        },
    );
    let (ws_addr, ws_handle) = af_async::spawn_with_opts(
        WsRouteActor {
            downstream_a: sink_a,
            downstream_b: sink_b,
        },
        af_async::SpawnOpts {
            mailbox_capacity: cfg.cap,
            ..Default::default()
        },
    );

    send_payloads(cfg.warmup, cfg.yield_every, &payloads, &ws_addr).await;

    while processed.load(Ordering::Relaxed) < cfg.warmup {
        tokio::task::yield_now().await;
    }
    processed.store(0, Ordering::Relaxed);
    alloc_reset();
    let cpu0 = cpu_time();
    let t0 = Instant::now();

    for idx in 0..cfg.msgs {
        let payload = payloads[(idx as usize) % payloads.len()].clone();
        while !ws_addr.tell(payload.clone()) {
            tokio::task::yield_now().await;
        }
        if cfg.yield_every > 0 && ((idx + 1) % cfg.yield_every == 0) {
            tokio::task::yield_now().await;
        }
    }

    while processed.load(Ordering::Relaxed) < cfg.msgs {
        tokio::task::yield_now().await;
    }

    let wall = t0.elapsed();
    let cpu = cpu_time().saturating_sub(cpu0);
    let stats = alloc_snapshot();
    print_alloc_stats_row("typed_route_2_same_thread", cfg.msgs, &stats);

    ws_handle.shutdown().await;
    sink_a_handle.shutdown().await;
    sink_b_handle.shutdown().await;
    (wall, cpu, cfg.msgs)
}

fn main() {
    let cfg = match parse_config() {
        Ok(cfg) => cfg,
        Err(msg) => {
            eprintln!("{msg}");
            return;
        }
    };

    println!(
        "config: msgs={} warmup={} cap={} yield_every={} mode={:?}",
        cfg.msgs, cfg.warmup, cfg.cap, cfg.yield_every, cfg.mode
    );
    println!("topology: current-thread Tokio runtime; producer + ws actor + downstream actor(s) all on one thread");
    println!();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread runtime");

    let (wall, cpu, msgs) = rt.block_on(async move {
        match cfg.mode {
            Mode::TypedSingleSameThread => run_single(cfg).await,
            Mode::TypedRoute2SameThread => run_route2(cfg).await,
        }
    });
    drop(rt);

    let wall_s = wall.as_secs_f64();
    let cpu_s = cpu.as_secs_f64();

    println!("msgs: {}  cap: {}", msgs, cfg.cap);
    println!(
        "wall: {}  cpu: {}  cpu_util: {:.2}",
        fmt_duration(wall),
        fmt_duration(cpu),
        if wall_s > 0.0 { cpu_s / wall_s } else { 0.0 }
    );
    println!(
        "throughput: {:.3} msg/s",
        if wall_s > 0.0 { msgs as f64 / wall_s } else { 0.0 }
    );
}
