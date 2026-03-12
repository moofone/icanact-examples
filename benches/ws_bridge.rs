mod _bench_util;

// This bench is only meaningful with the `ws` feature enabled (brings in `shared-ws`, `bytes`,
// and Tokio in the same way as production's async edge).
//
// Keep the non-`ws` build path compiling so `cargo bench` without `--features ws` still works.

#[cfg(not(feature = "ws"))]
fn main() {
    eprintln!("ws_bridge bench requires: cargo bench --features ws --bench ws_bridge");
}

#[cfg(feature = "ws")]
mod impl_ws {
    use super::_bench_util::{cpu_time, fmt_duration, spin_then_yield};

    use bytes::Bytes;
    use icanact_core::local_sync;
    use serde::Deserialize;
    use shared_ws::{
        client::{accept_async, connect_async},
        ws::{WsFrame, WsText},
    };
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
        msgs: u64,
        warmup: u64,
        cap: usize,
        threads: usize,
        mode: Mode,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Mode {
        BytesToSync,
        TypedToSync,
    }

    fn usage() -> &'static str {
        "Usage: cargo bench -p icanact-core --features ws --bench ws_bridge -- [options]\n\
         \n\
         Options:\n\
           --msgs N        Measurement messages (default: 2_000_000)\n\
           --warmup N      Warmup messages (default: 50_000)\n\
           --cap N         Mailbox capacity (default: 65_536)\n\
           --threads N     Tokio worker threads (default: 2)\n\
           --mode MODE     bytes_to_sync | typed_to_sync (default: bytes_to_sync)\n"
    }

    fn parse_u64(s: &str) -> Result<u64, String> {
        s.parse::<u64>().map_err(|e| e.to_string())
    }

    fn parse_usize(s: &str) -> Result<usize, String> {
        s.parse::<usize>().map_err(|e| e.to_string())
    }

    fn parse_mode(s: &str) -> Result<Mode, String> {
        match s {
            "bytes_to_sync" => Ok(Mode::BytesToSync),
            "typed_to_sync" => Ok(Mode::TypedToSync),
            other => Err(format!("invalid --mode: {other}")),
        }
    }

    fn parse_config() -> Result<Config, String> {
        let mut cfg = Config {
            msgs: 2_000_000,
            warmup: 50_000,
            cap: 65_536,
            threads: 2,
            mode: Mode::BytesToSync,
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

    async fn ws_mock_server_run(
        listener: tokio::net::TcpListener,
        warmup: u64,
        msgs: u64,
        payloads: Arc<Vec<Bytes>>,
    ) {
        let (stream, _addr) = listener.accept().await.expect("accept");
        let mut ws = accept_async(stream).await.expect("ws accept");

        // Initial subscribe message (ignored).
        let _ = ws.next().await;

        // Warmup frames.
        for i in 0..warmup {
            let b = payloads[(i as usize) % payloads.len()].clone();
            let frame = WsFrame::Text(unsafe { WsText::from_bytes_unchecked(b) });
            ws.send(frame).await.expect("server send warmup");
        }

        // Barrier: wait for client GO.
        let _ = ws.next().await;

        // Measurement frames.
        for i in 0..msgs {
            let b = payloads[(i as usize) % payloads.len()].clone();
            let frame = WsFrame::Text(unsafe { WsText::from_bytes_unchecked(b) });
            ws.send(frame).await.expect("server send");
        }
    }

    async fn run_ws_bridge(cfg: Config) -> (Duration, Duration, u64) {
        // Prebuilt JSON payloads (valid UTF-8). Server sends these as text frames.
        // Use a small rotating set of payloads to avoid measuring per-message allocation.
        const PAYLOADS: usize = 128;
        let mut v = Vec::with_capacity(PAYLOADS);
        for i in 0..PAYLOADS {
            let s = format!(
                "{{\"sym\":\"SYM{idx}\",\"ts\":1700000000000,\"price\":123.45,\"qty\":0.67}}",
                idx = i
            );
            v.push(Bytes::from(s));
        }
        let payloads: Arc<Vec<Bytes>> = Arc::new(v);

        let processed = Arc::new(AtomicU64::new(0));
        let processed_w = Arc::clone(&processed);

        let warmup = cfg.warmup;
        let msgs = cfg.msgs;

        // Sync actor either parses bytes or consumes typed TradeLite.
        let (addr_bytes, addr_typed, handle) = match cfg.mode {
            Mode::BytesToSync => {
                let (addr, h) = local_sync::mpsc::spawn(cfg.cap, move |msg: Bytes| {
                    let t = parse_trade_from_json_bytes(&msg);
                    std::hint::black_box((t.symbol, t.ts_ms, t.price, t.qty));
                    processed_w.fetch_add(1, Ordering::Relaxed);
                });
                (Some(addr), None, h)
            }
            Mode::TypedToSync => {
                let (addr, h) = local_sync::mpsc::spawn(cfg.cap, move |t: TradeLite| {
                    std::hint::black_box((t.symbol, t.ts_ms, t.price, t.qty));
                    processed_w.fetch_add(1, Ordering::Relaxed);
                });
                (None, Some(addr), h)
            }
        };

        let cpu0 = cpu_time();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr_listener = listener.local_addr().expect("local addr");

        let server = tokio::spawn(ws_mock_server_run(
            listener,
            warmup,
            msgs,
            Arc::clone(&payloads),
        ));

        let url = format!("ws://{}", addr_listener);
        let mut client = connect_async(url).await.expect("connect");

        // Subscribe (dummy message for the server barrier).
        let sub = Bytes::from_static(b"{\"op\":\"subscribe\",\"args\":[\"all\"]}");
        client
            .send(WsFrame::Text(unsafe { WsText::from_bytes_unchecked(sub) }))
            .await
            .expect("sub send");

        // Consume warmup frames.
        for _ in 0..warmup {
            let frame = client.next().await.expect("warm frame").expect("warm ok");
            let bytes = match frame {
                WsFrame::Text(t) => t.into_bytes(),
                WsFrame::Binary(b) => b,
                _ => continue,
            };

            match cfg.mode {
                Mode::BytesToSync => {
                    let addr = addr_bytes.as_ref().unwrap();
                    let mut spins = 0u32;
                    let mut msg = bytes;
                    loop {
                        match addr.try_tell(msg) {
                            Ok(()) => break,
                            Err(returned) => {
                                msg = returned;
                                spin_then_yield(&mut spins);
                            }
                        }
                    }
                }
                Mode::TypedToSync => {
                    let t = parse_trade_from_json_bytes(&bytes);
                    let addr = addr_typed.as_ref().unwrap();
                    let mut spins = 0u32;
                    let mut msg = t;
                    loop {
                        match addr.try_tell(msg) {
                            Ok(()) => break,
                            Err(returned) => {
                                msg = returned;
                                spin_then_yield(&mut spins);
                            }
                        }
                    }
                }
            }
        }

        while processed.load(Ordering::Relaxed) < warmup {
            std::hint::spin_loop();
        }
        processed.store(0, Ordering::Relaxed);

        // GO barrier.
        let go = Bytes::from_static(b"{\"op\":\"go\"}");
        client
            .send(WsFrame::Text(unsafe { WsText::from_bytes_unchecked(go) }))
            .await
            .expect("go send");

        let t0 = Instant::now();

        // Consume measurement frames.
        for _ in 0..msgs {
            let frame = client.next().await.expect("meas frame").expect("meas ok");
            let bytes = match frame {
                WsFrame::Text(t) => t.into_bytes(),
                WsFrame::Binary(b) => b,
                _ => continue,
            };

            match cfg.mode {
                Mode::BytesToSync => {
                    let addr = addr_bytes.as_ref().unwrap();
                    let mut spins = 0u32;
                    let mut msg = bytes;
                    loop {
                        match addr.try_tell(msg) {
                            Ok(()) => break,
                            Err(returned) => {
                                msg = returned;
                                spin_then_yield(&mut spins);
                            }
                        }
                    }
                }
                Mode::TypedToSync => {
                    let t = parse_trade_from_json_bytes(&bytes);
                    let addr = addr_typed.as_ref().unwrap();
                    let mut spins = 0u32;
                    let mut msg = t;
                    loop {
                        match addr.try_tell(msg) {
                            Ok(()) => break,
                            Err(returned) => {
                                msg = returned;
                                spin_then_yield(&mut spins);
                            }
                        }
                    }
                }
            }
        }

        while processed.load(Ordering::Relaxed) < msgs {
            std::hint::spin_loop();
        }

        let wall = t0.elapsed();
        let _ = server.await;

        handle.shutdown();

        let cpu = cpu_time().saturating_sub(cpu0);
        (wall, cpu, msgs)
    }

    pub fn main() {
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
        println!();

        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(cfg.threads)
            .enable_all()
            .build()
            .expect("tokio runtime");

        let (wall, cpu, msgs) = rt.block_on(run_ws_bridge(cfg));
        drop(rt);

        let wall_s = wall.as_secs_f64();
        let cpu_s = cpu.as_secs_f64();

        println!("== icanact-core ws bridge (shared-ws client) ==");
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
}

#[cfg(feature = "ws")]
fn main() {
    impl_ws::main();
}
