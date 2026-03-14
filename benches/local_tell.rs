mod _bench_util;

use _bench_util::{alloc_reset, alloc_snapshot, print_alloc_stats_row};
use icanact_core::local_sync as sync;
use std::{
    env,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

#[derive(Clone, Copy)]
struct Config {
    msgs: u64,
    warmup: u64,
    cap: usize,
    producers: usize,
}

fn usage() -> &'static str {
    "Usage: cargo bench -p icanact-core --bench local_tell -- [options]\n\
     \n\
     Options:\n\
       --msgs N     Number of messages (default: 10_000_000)\n\
       --warmup N   Warmup messages (default: 50_000)\n\
       --cap N      Mailbox capacity (default: 65_536)\n\
       --producers N Number of producer threads (default: 1)\n"
}

fn parse_u64(s: &str) -> Result<u64, String> {
    s.parse::<u64>().map_err(|e| e.to_string())
}

fn parse_usize(s: &str) -> Result<usize, String> {
    s.parse::<usize>().map_err(|e| e.to_string())
}

fn parse_config() -> Result<Config, String> {
    let mut cfg = Config {
        msgs: 10_000_000,
        warmup: 50_000,
        cap: 65_536,
        producers: 1,
    };

    let mut it = env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            // Cargo may pass libtest flags even for `harness = false` benches.
            "--bench" => {}
            "--help" | "-h" => return Err(usage().to_string()),
            "--msgs" => {
                let v = it
                    .next()
                    .ok_or_else(|| "missing value for --msgs".to_string())
                    .and_then(|s| parse_u64(&s))?;
                cfg.msgs = v;
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
            "--producers" => {
                let v = it
                    .next()
                    .ok_or_else(|| "missing value for --producers".to_string())
                    .and_then(|s| parse_usize(&s))?;
                cfg.producers = v.max(1);
            }
            other => return Err(format!("unknown arg: {other}\n\n{}", usage())),
        }
    }

    Ok(cfg)
}

fn send_n(addr: &sync::MailboxAddr<u64>, n: u64) {
    for _ in 0..n {
        loop {
            match addr.try_tell(1u64) {
                Ok(()) => break,
                Err(_msg) => std::hint::spin_loop(),
            }
        }
    }
}

fn run_producers(addr: &sync::MailboxAddr<u64>, total: u64, producers: usize) {
    if producers <= 1 {
        send_n(addr, total);
        return;
    }

    let p = producers as u64;
    let base = total / p;
    let rem = total % p;

    let mut joins = Vec::with_capacity(producers);
    for i in 0..producers {
        let a = addr.clone();
        let mut n = base;
        if (i as u64) < rem {
            n += 1;
        }
        joins.push(thread::spawn(move || send_n(&a, n)));
    }

    for j in joins {
        j.join().expect("producer thread panicked");
    }
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
    if secs < 1e-3 {
        format!("{:.3}us", secs * 1e6)
    } else if secs < 1.0 {
        format!("{:.3}ms", secs * 1e3)
    } else {
        format!("{:.3}s", secs)
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

    #[cfg(feature = "perf-counters")]
    sync::counters_reset();

    let processed = Arc::new(AtomicU64::new(0));
    let processed_for_actor = Arc::clone(&processed);
    let (addr, handle) = sync::mpsc::spawn(cfg.cap, move |_msg: u64| {
        processed_for_actor.fetch_add(1, Ordering::Relaxed);
    });

    // Warmup.
    run_producers(&addr, cfg.warmup, cfg.producers);
    while processed.load(Ordering::Relaxed) < cfg.warmup {
        std::hint::spin_loop();
    }
    processed.store(0, Ordering::Relaxed);
    alloc_reset();

    let cpu0 = cpu_time();
    let t0 = Instant::now();

    run_producers(&addr, cfg.msgs, cfg.producers);

    while processed.load(Ordering::Relaxed) < cfg.msgs {
        thread::yield_now();
    }

    let wall = t0.elapsed();
    let cpu = cpu_time().saturating_sub(cpu0);
    handle.shutdown();

    let wall_s = wall.as_secs_f64();
    let cpu_s = cpu.as_secs_f64();

    println!("== icanact-core local tell end-to-end ==");
    println!(
        "msgs: {}  cap: {}  producers: {}",
        cfg.msgs, cfg.cap, cfg.producers
    );
    println!(
        "wall: {}  cpu: {}  cpu_util: {:.2}",
        fmt_duration(wall),
        fmt_duration(cpu),
        if wall_s > 0.0 { cpu_s / wall_s } else { 0.0 }
    );
    println!(
        "throughput: {:.3} msg/s",
        if wall_s > 0.0 {
            cfg.msgs as f64 / wall_s
        } else {
            0.0
        }
    );
    let alloc = alloc_snapshot();
    print_alloc_stats_row("local_tell", cfg.msgs, &alloc);

    #[cfg(feature = "perf-counters")]
    {
        let counters = sync::counters_snapshot();
        println!("perf_counters_enabled: {}", sync::COUNTERS_ENABLED);
        println!("perf_counters.mpsc: {:?}", counters.mpsc);
        println!("perf_counters.ask: {:?}", counters.ask);
    }
}
