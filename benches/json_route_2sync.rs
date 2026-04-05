mod _bench_util;

use _bench_util::{
    LatencyBuf, alloc_reset, alloc_snapshot, cpu_time, fmt_duration, latency_stats_from_nanos,
    print_alloc_stats_row, print_latency_row, spin_then_yield,
};
use bytes::Bytes;
use icanact_core::local_sync;
use serde::Deserialize;
use sonic_rs::{JsonValueTrait, Value};
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
    threads: usize,
    mode: Mode,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    RawBytesRoute2Sync,
    OwnedJsonRoute2Sync,
    TypedStructRoute2Sync,
}

fn usage() -> &'static str {
    "Usage: cargo bench --bench json_route_2sync -- [options]\n\
     \n\
     Options:\n\
       --msgs N        Measurement messages (default: 2_000_000)\n\
       --warmup N      Warmup messages (default: 50_000)\n\
       --cap N         Mailbox capacity (default: 65_536)\n\
       --threads N     Tokio worker threads (default: 1)\n\
       --mode MODE     raw_bytes_route_2sync | owned_json_route_2sync | typed_struct_route_2sync (default: owned_json_route_2sync)\n"
}

fn parse_u64(s: &str) -> Result<u64, String> {
    s.parse::<u64>().map_err(|e| e.to_string())
}

fn parse_usize(s: &str) -> Result<usize, String> {
    s.parse::<usize>().map_err(|e| e.to_string())
}

fn parse_mode(s: &str) -> Result<Mode, String> {
    match s {
        "raw_bytes_route_2sync" => Ok(Mode::RawBytesRoute2Sync),
        "owned_json_route_2sync" => Ok(Mode::OwnedJsonRoute2Sync),
        "typed_struct_route_2sync" => Ok(Mode::TypedStructRoute2Sync),
        other => Err(format!("invalid --mode: {other}")),
    }
}

fn parse_config() -> Result<Config, String> {
    let mut cfg = Config {
        msgs: 2_000_000,
        warmup: 50_000,
        cap: 65_536,
        threads: 1,
        mode: Mode::OwnedJsonRoute2Sync,
    };

    let mut it = env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--bench" => {}
            "--help" | "-h" => return Err(usage().to_string()),
            "--msgs" => cfg.msgs = parse_u64(&it.next().ok_or("missing --msgs")?)?,
            "--warmup" => cfg.warmup = parse_u64(&it.next().ok_or("missing --warmup")?)?,
            "--cap" => cfg.cap = parse_usize(&it.next().ok_or("missing --cap")?)?,
            "--threads" => cfg.threads = parse_usize(&it.next().ok_or("missing --threads")?)?,
            "--mode" => cfg.mode = parse_mode(&it.next().ok_or("missing --mode")?)?,
            other => return Err(format!("unknown arg: {other}\n\n{}", usage())),
        }
    }

    if cfg.threads == 0 {
        return Err("--threads must be >= 1".to_string());
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

struct RawBytesMsg {
    payload: Bytes,
    sampled_at: Option<Instant>,
}

struct OwnedJsonMsg {
    value: Value,
    sampled_at: Option<Instant>,
}

#[derive(Clone, Copy, Debug)]
struct TypedTradeMsg {
    trade: TradeLite,
    sampled_at: Option<Instant>,
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

#[inline(always)]
fn parse_owned_value(payload: &[u8]) -> Value {
    sonic_rs::from_slice(payload).expect("valid owned json value")
}

#[inline(always)]
fn lane_from_symbol(sym: &str) -> usize {
    (symbol_id(sym) as usize) & 1
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

fn spawn_owned_json_actors(
    cap: usize,
    processed: &Arc<AtomicU64>,
    latency: &Arc<LatencyBuf>,
) -> (
    Vec<local_sync::MailboxAddr<OwnedJsonMsg>>,
    Vec<local_sync::mpsc::ActorHandle>,
) {
    let mut addrs = Vec::with_capacity(2);
    let mut handles = Vec::with_capacity(2);
    for _ in 0..2 {
        let processed = Arc::clone(processed);
        let latency = Arc::clone(latency);
        let (addr, handle) = local_sync::mpsc::spawn(cap, move |msg: OwnedJsonMsg| {
            let sym_value = msg.value.get("sym");
            let sym = sym_value.as_str().expect("sym");
            let ts = msg.value.get("ts").as_u64().expect("ts");
            let price = msg.value.get("price").as_f64().expect("price");
            let qty = msg.value.get("qty").as_f64().expect("qty");
            std::hint::black_box((sym, ts, price, qty));
            if let Some(sampled_at) = msg.sampled_at {
                latency.record(sampled_at.elapsed().as_nanos() as u64);
            }
            processed.fetch_add(1, Ordering::Relaxed);
        });
        addrs.push(addr);
        handles.push(handle);
    }
    (addrs, handles)
}

fn spawn_raw_bytes_actors(
    cap: usize,
    processed: &Arc<AtomicU64>,
    latency: &Arc<LatencyBuf>,
) -> (
    Vec<local_sync::MailboxAddr<RawBytesMsg>>,
    Vec<local_sync::mpsc::ActorHandle>,
) {
    let mut addrs = Vec::with_capacity(2);
    let mut handles = Vec::with_capacity(2);
    for _ in 0..2 {
        let processed = Arc::clone(processed);
        let latency = Arc::clone(latency);
        let (addr, handle) = local_sync::mpsc::spawn(cap, move |msg: RawBytesMsg| {
            let trade = parse_trade_from_json_bytes(&msg.payload);
            std::hint::black_box((trade.symbol, trade.ts_ms, trade.price, trade.qty));
            if let Some(sampled_at) = msg.sampled_at {
                latency.record(sampled_at.elapsed().as_nanos() as u64);
            }
            processed.fetch_add(1, Ordering::Relaxed);
        });
        addrs.push(addr);
        handles.push(handle);
    }
    (addrs, handles)
}

fn spawn_typed_actors(
    cap: usize,
    processed: &Arc<AtomicU64>,
    latency: &Arc<LatencyBuf>,
) -> (
    Vec<local_sync::MailboxAddr<TypedTradeMsg>>,
    Vec<local_sync::mpsc::ActorHandle>,
) {
    let mut addrs = Vec::with_capacity(2);
    let mut handles = Vec::with_capacity(2);
    for _ in 0..2 {
        let processed = Arc::clone(processed);
        let latency = Arc::clone(latency);
        let (addr, handle) = local_sync::mpsc::spawn(cap, move |msg: TypedTradeMsg| {
            std::hint::black_box((msg.trade.symbol, msg.trade.ts_ms, msg.trade.price, msg.trade.qty));
            if let Some(sampled_at) = msg.sampled_at {
                latency.record(sampled_at.elapsed().as_nanos() as u64);
            }
            processed.fetch_add(1, Ordering::Relaxed);
        });
        addrs.push(addr);
        handles.push(handle);
    }
    (addrs, handles)
}

#[inline(always)]
fn route_lane_from_json_bytes(payload: &[u8]) -> usize {
    lane_from_symbol(
        payload
            .windows(4)
            .position(|w| w == b"SYM0" || w == b"SYM1")
            .map(|idx| {
                if payload[idx + 3] == b'0' {
                    "SYM0"
                } else {
                    "SYM1"
                }
            })
            .expect("route symbol present"),
    )
}

async fn run_raw_bytes_route_2sync(
    cfg: Config,
) -> (std::time::Duration, std::time::Duration, u64) {
    let payloads = build_payloads();
    let processed = Arc::new(AtomicU64::new(0));
    let latency = Arc::new(LatencyBuf::new(((cfg.msgs / 1024).max(1)) as usize + 1));
    let (addrs, handles) = spawn_raw_bytes_actors(cfg.cap, &processed, &latency);

    for idx in 0..cfg.warmup {
        let payload = payloads[(idx as usize) % payloads.len()].clone();
        let lane = route_lane_from_json_bytes(&payload);
        let addr = &addrs[lane];
        let mut spins = 0u32;
        let mut pending = RawBytesMsg {
            payload,
            sampled_at: None,
        };
        loop {
            match addr.try_tell(pending) {
                Ok(()) => break,
                Err(returned) => {
                    pending = returned;
                    spin_then_yield(&mut spins);
                }
            }
        }
    }

    while processed.load(Ordering::Relaxed) < cfg.warmup {
        std::hint::spin_loop();
    }
    processed.store(0, Ordering::Relaxed);
    latency.reset();
    alloc_reset();
    let cpu0 = cpu_time();
    let t0 = Instant::now();

    for idx in 0..cfg.msgs {
        let payload = payloads[(idx as usize) % payloads.len()].clone();
        let lane = route_lane_from_json_bytes(&payload);
        let addr = &addrs[lane];
        let mut spins = 0u32;
        let mut pending = RawBytesMsg {
            payload,
            sampled_at: if idx & 1023 == 0 {
                Some(Instant::now())
            } else {
                None
            },
        };
        loop {
            match addr.try_tell(pending) {
                Ok(()) => break,
                Err(returned) => {
                    pending = returned;
                    tokio::task::yield_now().await;
                    spin_then_yield(&mut spins);
                }
            }
        }
    }

    while processed.load(Ordering::Relaxed) < cfg.msgs {
        std::hint::spin_loop();
    }

    let wall = t0.elapsed();
    let cpu = cpu_time().saturating_sub(cpu0);
    let stats = alloc_snapshot();
    print_alloc_stats_row("raw_bytes_route_2sync", cfg.msgs, &stats);
    print_latency_row("raw_bytes_route_2sync_latency", &latency_stats_from_nanos(latency.collect()));
    for handle in handles {
        handle.shutdown();
    }
    (wall, cpu, cfg.msgs)
}

async fn run_owned_json_route_2sync(cfg: Config) -> (std::time::Duration, std::time::Duration, u64) {
    let payloads = build_payloads();
    let processed = Arc::new(AtomicU64::new(0));
    let latency = Arc::new(LatencyBuf::new(((cfg.msgs / 1024).max(1)) as usize + 1));
    let (addrs, handles) = spawn_owned_json_actors(cfg.cap, &processed, &latency);

    for idx in 0..cfg.warmup {
        let payload = &payloads[(idx as usize) % payloads.len()];
        let value = parse_owned_value(payload);
        let lane = lane_from_symbol(value.get("sym").as_str().expect("sym"));
        let addr = &addrs[lane];
        let mut spins = 0u32;
        let mut pending = OwnedJsonMsg {
            value,
            sampled_at: None,
        };
        loop {
            match addr.try_tell(pending) {
                Ok(()) => break,
                Err(returned) => {
                    pending = returned;
                    spin_then_yield(&mut spins);
                }
            }
        }
    }

    while processed.load(Ordering::Relaxed) < cfg.warmup {
        std::hint::spin_loop();
    }
    processed.store(0, Ordering::Relaxed);
    latency.reset();
    alloc_reset();
    let cpu0 = cpu_time();
    let t0 = Instant::now();

    for idx in 0..cfg.msgs {
        let payload = &payloads[(idx as usize) % payloads.len()];
        let value = parse_owned_value(payload);
        let lane = lane_from_symbol(value.get("sym").as_str().expect("sym"));
        let addr = &addrs[lane];
        let mut spins = 0u32;
        let mut pending = OwnedJsonMsg {
            value,
            sampled_at: if idx & 1023 == 0 {
                Some(Instant::now())
            } else {
                None
            },
        };
        loop {
            match addr.try_tell(pending) {
                Ok(()) => break,
                Err(returned) => {
                    pending = returned;
                    tokio::task::yield_now().await;
                    spin_then_yield(&mut spins);
                }
            }
        }
    }

    while processed.load(Ordering::Relaxed) < cfg.msgs {
        std::hint::spin_loop();
    }

    let wall = t0.elapsed();
    let cpu = cpu_time().saturating_sub(cpu0);
    let stats = alloc_snapshot();
    print_alloc_stats_row("owned_json_route_2sync", cfg.msgs, &stats);
    print_latency_row("owned_json_route_2sync_latency", &latency_stats_from_nanos(latency.collect()));
    for handle in handles {
        handle.shutdown();
    }
    (wall, cpu, cfg.msgs)
}

async fn run_typed_struct_route_2sync(
    cfg: Config,
) -> (std::time::Duration, std::time::Duration, u64) {
    let payloads = build_payloads();
    let processed = Arc::new(AtomicU64::new(0));
    let latency = Arc::new(LatencyBuf::new(((cfg.msgs / 1024).max(1)) as usize + 1));
    let (addrs, handles) = spawn_typed_actors(cfg.cap, &processed, &latency);

    for idx in 0..cfg.warmup {
        let payload = &payloads[(idx as usize) % payloads.len()];
        let trade = parse_trade_from_json_bytes(payload);
        let addr = &addrs[(trade.symbol as usize) & 1];
        let mut spins = 0u32;
        let mut pending = TypedTradeMsg {
            trade,
            sampled_at: None,
        };
        loop {
            match addr.try_tell(pending) {
                Ok(()) => break,
                Err(returned) => {
                    pending = returned;
                    spin_then_yield(&mut spins);
                }
            }
        }
    }

    while processed.load(Ordering::Relaxed) < cfg.warmup {
        std::hint::spin_loop();
    }
    processed.store(0, Ordering::Relaxed);
    latency.reset();
    alloc_reset();
    let cpu0 = cpu_time();
    let t0 = Instant::now();

    for idx in 0..cfg.msgs {
        let payload = &payloads[(idx as usize) % payloads.len()];
        let trade = parse_trade_from_json_bytes(payload);
        let addr = &addrs[(trade.symbol as usize) & 1];
        let mut spins = 0u32;
        let mut pending = TypedTradeMsg {
            trade,
            sampled_at: if idx & 1023 == 0 {
                Some(Instant::now())
            } else {
                None
            },
        };
        loop {
            match addr.try_tell(pending) {
                Ok(()) => break,
                Err(returned) => {
                    pending = returned;
                    tokio::task::yield_now().await;
                    spin_then_yield(&mut spins);
                }
            }
        }
    }

    while processed.load(Ordering::Relaxed) < cfg.msgs {
        std::hint::spin_loop();
    }

    let wall = t0.elapsed();
    let cpu = cpu_time().saturating_sub(cpu0);
    let stats = alloc_snapshot();
    print_alloc_stats_row("typed_struct_route_2sync", cfg.msgs, &stats);
    print_latency_row("typed_struct_route_2sync_latency", &latency_stats_from_nanos(latency.collect()));
    for handle in handles {
        handle.shutdown();
    }
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
        "config: msgs={} warmup={} cap={} threads={} mode={:?}",
        cfg.msgs, cfg.warmup, cfg.cap, cfg.threads, cfg.mode
    );
    println!("topology: 1 producer task -> 2 sync actors, routed 50/50 by sym");
    println!();

    let rt = if cfg.threads <= 1 {
        tokio::runtime::Builder::new_current_thread()
    } else {
        let mut b = tokio::runtime::Builder::new_multi_thread();
        b.worker_threads(cfg.threads);
        b
    }
    .enable_all()
    .build()
    .expect("tokio runtime");

    let (wall, cpu, msgs) = rt.block_on(async move {
        match cfg.mode {
            Mode::RawBytesRoute2Sync => run_raw_bytes_route_2sync(cfg).await,
            Mode::OwnedJsonRoute2Sync => run_owned_json_route_2sync(cfg).await,
            Mode::TypedStructRoute2Sync => run_typed_struct_route_2sync(cfg).await,
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
        if wall_s > 0.0 {
            msgs as f64 / wall_s
        } else {
            0.0
        }
    );
}
