mod _bench_util;

use std::env;
use std::time::Instant;

use icanact_core::local_sync;

#[derive(Clone, Copy)]
struct Config {
    asks: u64,
    warmup: u64,
    cap: usize,
}

fn parse_u64(s: &str) -> Result<u64, String> {
    s.parse::<u64>().map_err(|e| e.to_string())
}

fn parse_usize(s: &str) -> Result<usize, String> {
    s.parse::<usize>().map_err(|e| e.to_string())
}

fn parse_config() -> Result<Config, String> {
    let mut cfg = Config {
        asks: 2_000_000,
        warmup: 20_000,
        cap: 65_536,
    };

    let mut it = env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--bench" => {}
            "--asks" => {
                cfg.asks = it
                    .next()
                    .ok_or_else(|| "missing value for --asks".to_string())
                    .and_then(|s| parse_u64(&s))?
                    .max(1);
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
                    .and_then(|s| parse_usize(&s))?
                    .max(1);
            }
            _ => {}
        }
    }
    Ok(cfg)
}

enum Msg {
    Echo {
        v: u64,
        reply: local_sync::ReplyTo<u64>,
    },
}

fn main() {
    let cfg = parse_config().expect("invalid config");
    let (addr, handle) = local_sync::mpsc::spawn(cfg.cap, move |msg: Msg| match msg {
        Msg::Echo { v, reply } => {
            let _ = reply.reply(v);
        }
    });

    for i in 0..cfg.warmup {
        let out = addr
            .ask(|reply| Msg::Echo { v: i, reply })
            .expect("warmup ask failed");
        assert_eq!(out, i);
    }

    let t0 = Instant::now();
    for i in 0..cfg.asks {
        let out = addr
            .ask(|reply| Msg::Echo { v: i, reply })
            .expect("ask failed");
        assert_eq!(out, i);
    }
    let elapsed = t0.elapsed();
    let throughput = cfg.asks as f64 / elapsed.as_secs_f64();

    println!("== ask_af_native ==");
    println!("asks={} warmup={} cap={}", cfg.asks, cfg.warmup, cfg.cap);
    println!("elapsed_ns={}", elapsed.as_nanos());
    println!("throughput: {:.3} ask/s", throughput);

    handle.shutdown();
}
