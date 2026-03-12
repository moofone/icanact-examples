mod _bench_util;

use _bench_util::{
    LatencyBuf, RateLimiter, alloc_reset, alloc_snapshot, decode_sample_t0, encode_sample_t0,
    latency_stats_from_nanos, now_ns, print_alloc_stats_row, print_latency_row, spin_then_yield,
};

use icanact_core::local_sync;
use std::{
    env,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::Instant,
};

#[derive(Clone, Copy)]
struct Config {
    msgs: u64,
    warmup: u64,
    cap: usize,
    rate: u64,
    sample_every: u64,
}

fn usage() -> &'static str {
    "Usage: cargo bench -p icanact-core --bench latency_local -- [options]\n\
     \n\
     Options:\n\
       --msgs N          Measurement messages (default: 200_000)\n\
       --warmup N        Warmup messages (default: 50_000)\n\
       --cap N           Mailbox capacity (default: 65_536)\n\
       --rate N          Offered load in msgs/sec (0 = max speed; default: 0)\n\
       --sample-every N  Sample every Nth message (default: 16)\n\
       --variant NAME    Optional: run only one variant: u64 | u64_tell_with | a2a_u64 | s128 | big1024 | big1024_tell_with\n"
}

fn parse_u64(s: &str) -> Result<u64, String> {
    s.parse::<u64>().map_err(|e| e.to_string())
}

fn parse_usize(s: &str) -> Result<usize, String> {
    s.parse::<usize>().map_err(|e| e.to_string())
}

fn parse_config() -> Result<(Config, Option<String>), String> {
    let mut cfg = Config {
        msgs: 200_000,
        warmup: 50_000,
        cap: 65_536,
        rate: 0,
        sample_every: 16,
    };
    let mut only = None::<String>;

    let mut it = env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--bench" => {}
            "--help" | "-h" => return Err(usage().to_string()),
            "--msgs" => cfg.msgs = parse_u64(&it.next().ok_or("missing --msgs")?)?,
            "--warmup" => cfg.warmup = parse_u64(&it.next().ok_or("missing --warmup")?)?,
            "--cap" => cfg.cap = parse_usize(&it.next().ok_or("missing --cap")?)?,
            "--rate" => cfg.rate = parse_u64(&it.next().ok_or("missing --rate")?)?,
            "--sample-every" => {
                cfg.sample_every = parse_u64(&it.next().ok_or("missing --sample-every")?)?
            }
            "--variant" => only = Some(it.next().ok_or("missing --variant")?),
            other => return Err(format!("unknown arg: {other}\n\n{}", usage())),
        }
    }

    Ok((cfg, only))
}

#[repr(C)]
#[derive(Clone, Copy)]
struct S128 {
    a: [u64; 16],
}

#[repr(C)]
#[derive(Clone, Copy)]
struct Big1024 {
    buf: [u8; 1024],
}

#[inline(always)]
fn big1024_get_t0(msg: &Big1024) -> Option<u64> {
    let mut b = [0u8; 8];
    b.copy_from_slice(&msg.buf[0..8]);
    decode_sample_t0(u64::from_le_bytes(b))
}

#[inline(always)]
fn big1024_set_t0(msg: &mut Big1024, v: u64) {
    msg.buf[0..8].copy_from_slice(&v.to_le_bytes());
}

fn bench_latency_u64(cfg: Config) -> _bench_util::LatencyStats {
    let sample_every = cfg.sample_every.max(1);
    let samples_cap = (cfg.msgs / sample_every).max(1) as usize + 64;
    let rec = Arc::new(LatencyBuf::new(samples_cap));

    let processed = Arc::new(AtomicU64::new(0));
    let processed_w = Arc::clone(&processed);

    let base = Instant::now();
    let base_w = base;
    let rec_w = Arc::clone(&rec);

    let (addr, handle) = local_sync::mpsc::spawn(cfg.cap, move |msg: u64| {
        if let Some(t0) = decode_sample_t0(msg) {
            let now = now_ns(base_w);
            rec_w.record(now.saturating_sub(t0));
        }
        processed_w.fetch_add(1, Ordering::Relaxed);
    });

    for _ in 0..cfg.warmup {
        let mut spins = 0u32;
        loop {
            match addr.try_tell(0) {
                Ok(()) => break,
                Err(_returned) => spin_then_yield(&mut spins),
            }
        }
    }
    while processed.load(Ordering::Relaxed) < cfg.warmup {
        std::hint::spin_loop();
    }
    processed.store(0, Ordering::Relaxed);
    rec.reset();

    let mut rl = RateLimiter::new(base, cfg.rate);
    for i in 0..cfg.msgs {
        rl.wait();
        let msg = if i % sample_every == 0 {
            encode_sample_t0(now_ns(base))
        } else {
            0
        };
        let mut spins = 0u32;
        loop {
            match addr.try_tell(msg) {
                Ok(()) => break,
                Err(_returned) => spin_then_yield(&mut spins),
            }
        }
    }

    while processed.load(Ordering::Relaxed) < cfg.msgs {
        std::hint::spin_loop();
    }

    handle.shutdown();
    latency_stats_from_nanos(rec.collect())
}

fn bench_latency_u64_tell_with(cfg: Config) -> _bench_util::LatencyStats {
    let sample_every = cfg.sample_every.max(1);
    let samples_cap = (cfg.msgs / sample_every).max(1) as usize + 64;
    let rec = Arc::new(LatencyBuf::new(samples_cap));

    let processed = Arc::new(AtomicU64::new(0));
    let processed_w = Arc::clone(&processed);

    let base = Instant::now();
    let base_w = base;
    let rec_w = Arc::clone(&rec);

    let (addr, handle) = local_sync::mpsc::spawn(cfg.cap, move |msg: u64| {
        if let Some(t0) = decode_sample_t0(msg) {
            let now = now_ns(base_w);
            rec_w.record(now.saturating_sub(t0));
        }
        processed_w.fetch_add(1, Ordering::Relaxed);
    });

    for _ in 0..cfg.warmup {
        let mut spins = 0u32;
        loop {
            match addr.tell_with(|p: *mut u64| unsafe { p.write(0) }) {
                Ok(()) => break,
                Err(()) => spin_then_yield(&mut spins),
            }
        }
    }
    while processed.load(Ordering::Relaxed) < cfg.warmup {
        std::hint::spin_loop();
    }
    processed.store(0, Ordering::Relaxed);
    rec.reset();

    let mut rl = RateLimiter::new(base, cfg.rate);
    for i in 0..cfg.msgs {
        rl.wait();
        let msg = if i % sample_every == 0 {
            encode_sample_t0(now_ns(base))
        } else {
            0
        };
        let mut spins = 0u32;
        loop {
            match addr.tell_with(|p: *mut u64| unsafe { p.write(msg) }) {
                Ok(()) => break,
                Err(()) => spin_then_yield(&mut spins),
            }
        }
    }

    while processed.load(Ordering::Relaxed) < cfg.msgs {
        std::hint::spin_loop();
    }

    handle.shutdown();
    latency_stats_from_nanos(rec.collect())
}

fn bench_latency_actor_to_actor_u64(cfg: Config) -> _bench_util::LatencyStats {
    #[derive(Clone, Copy)]
    enum SenderCmd {
        Warmup(u64),
        Run(u64),
    }

    let sample_every = cfg.sample_every.max(1);
    let samples_cap = (cfg.msgs / sample_every).max(1) as usize + 64;
    let rec = Arc::new(LatencyBuf::new(samples_cap));

    let base = Instant::now();
    let base_recv = base;
    let base_send = base;

    let processed = Arc::new(AtomicU64::new(0));
    let processed_recv = Arc::clone(&processed);
    let rec_recv = Arc::clone(&rec);

    let main_thread = thread::current();
    let stage = Arc::new(std::sync::atomic::AtomicU8::new(if cfg.warmup > 0 {
        0
    } else {
        1
    })); // 0 warmup, 1 run, 2 done
    let stage_recv = Arc::clone(&stage);

    let warmup_target = cfg.warmup;
    let run_target = cfg.msgs;

    let (recv, recv_handle) = local_sync::mpsc::spawn(cfg.cap, move |msg: u64| {
        if let Some(t0) = decode_sample_t0(msg) {
            let now = now_ns(base_recv);
            rec_recv.record(now.saturating_sub(t0));
        }

        let n = processed_recv.fetch_add(1, Ordering::Relaxed) + 1;
        let st = stage_recv.load(Ordering::Acquire);

        if st == 0 && warmup_target > 0 && n == warmup_target {
            processed_recv.store(0, Ordering::Relaxed);
            stage_recv.store(1, Ordering::Release);
            main_thread.unpark();
        } else if st == 1 && n == run_target {
            stage_recv.store(2, Ordering::Release);
            main_thread.unpark();
        }
    });

    let (sender, sender_handle) = {
        let recv = recv.clone();
        local_sync::mpsc::spawn(8, move |cmd: SenderCmd| {
            let mut rl = RateLimiter::new(base_send, cfg.rate);
            let n = match cmd {
                SenderCmd::Warmup(n) => n,
                SenderCmd::Run(n) => n,
            };
            for i in 0..n {
                rl.wait();
                let msg = match cmd {
                    SenderCmd::Warmup(_) => 0,
                    SenderCmd::Run(_) => {
                        if i % sample_every == 0 {
                            encode_sample_t0(now_ns(base_send))
                        } else {
                            0
                        }
                    }
                };

                let mut spins = 0u32;
                loop {
                    match recv.try_tell(msg) {
                        Ok(()) => break,
                        Err(_msg) => spin_then_yield(&mut spins),
                    }
                }
            }
        })
    };

    if cfg.warmup > 0 {
        stage.store(0, Ordering::Release);
        let mut spins = 0u32;
        loop {
            match sender.try_tell(SenderCmd::Warmup(cfg.warmup)) {
                Ok(()) => break,
                Err(_cmd) => spin_then_yield(&mut spins),
            }
        }
        while stage.load(Ordering::Acquire) < 1 {
            thread::park();
        }
    } else {
        stage.store(1, Ordering::Release);
    }

    rec.reset();

    let mut spins = 0u32;
    loop {
        match sender.try_tell(SenderCmd::Run(cfg.msgs)) {
            Ok(()) => break,
            Err(_cmd) => spin_then_yield(&mut spins),
        }
    }
    while stage.load(Ordering::Acquire) < 2 {
        thread::park();
    }

    sender_handle.shutdown();
    recv_handle.shutdown();
    drop(sender);
    drop(recv);

    latency_stats_from_nanos(rec.collect())
}

fn bench_latency_s128(cfg: Config) -> _bench_util::LatencyStats {
    let sample_every = cfg.sample_every.max(1);
    let samples_cap = (cfg.msgs / sample_every).max(1) as usize + 64;
    let rec = Arc::new(LatencyBuf::new(samples_cap));

    let processed = Arc::new(AtomicU64::new(0));
    let processed_w = Arc::clone(&processed);

    let base = Instant::now();
    let base_w = base;
    let rec_w = Arc::clone(&rec);

    let (addr, handle) = local_sync::mpsc::spawn(cfg.cap, move |msg: S128| {
        if let Some(t0) = decode_sample_t0(msg.a[0]) {
            let now = now_ns(base_w);
            rec_w.record(now.saturating_sub(t0));
        }
        std::hint::black_box(msg.a[1]);
        processed_w.fetch_add(1, Ordering::Relaxed);
    });

    let mut msg = S128 { a: [0u64; 16] };
    msg.a[1] = 7;

    for _ in 0..cfg.warmup {
        msg.a[0] = 0;
        let mut spins = 0u32;
        loop {
            match addr.try_tell(msg) {
                Ok(()) => break,
                Err(_returned) => spin_then_yield(&mut spins),
            }
        }
    }
    while processed.load(Ordering::Relaxed) < cfg.warmup {
        std::hint::spin_loop();
    }
    processed.store(0, Ordering::Relaxed);
    rec.reset();

    let mut rl = RateLimiter::new(base, cfg.rate);
    for i in 0..cfg.msgs {
        rl.wait();
        msg.a[0] = if i % sample_every == 0 {
            encode_sample_t0(now_ns(base))
        } else {
            0
        };

        let mut spins = 0u32;
        loop {
            match addr.try_tell(msg) {
                Ok(()) => break,
                Err(_returned) => spin_then_yield(&mut spins),
            }
        }
    }

    while processed.load(Ordering::Relaxed) < cfg.msgs {
        std::hint::spin_loop();
    }

    handle.shutdown();
    latency_stats_from_nanos(rec.collect())
}

fn bench_latency_big1024(cfg: Config) -> _bench_util::LatencyStats {
    let sample_every = cfg.sample_every.max(1);
    let samples_cap = (cfg.msgs / sample_every).max(1) as usize + 64;
    let rec = Arc::new(LatencyBuf::new(samples_cap));

    let processed = Arc::new(AtomicU64::new(0));
    let processed_w = Arc::clone(&processed);

    let base = Instant::now();
    let base_w = base;
    let rec_w = Arc::clone(&rec);

    let (addr, handle) = local_sync::mpsc::spawn(cfg.cap, move |msg: Big1024| {
        if let Some(t0) = big1024_get_t0(&msg) {
            let now = now_ns(base_w);
            rec_w.record(now.saturating_sub(t0));
        }
        std::hint::black_box(msg.buf[8]);
        processed_w.fetch_add(1, Ordering::Relaxed);
    });

    let mut msg = Big1024 { buf: [0u8; 1024] };
    msg.buf[16] = 7;

    for _ in 0..cfg.warmup {
        big1024_set_t0(&mut msg, 0);
        let mut spins = 0u32;
        loop {
            match addr.try_tell(msg) {
                Ok(()) => break,
                Err(_returned) => spin_then_yield(&mut spins),
            }
        }
    }
    while processed.load(Ordering::Relaxed) < cfg.warmup {
        std::hint::spin_loop();
    }
    processed.store(0, Ordering::Relaxed);
    rec.reset();

    let mut rl = RateLimiter::new(base, cfg.rate);
    for i in 0..cfg.msgs {
        rl.wait();
        let t0 = if i % sample_every == 0 {
            encode_sample_t0(now_ns(base))
        } else {
            0
        };
        big1024_set_t0(&mut msg, t0);

        let mut spins = 0u32;
        loop {
            match addr.try_tell(msg) {
                Ok(()) => break,
                Err(_returned) => spin_then_yield(&mut spins),
            }
        }
    }

    while processed.load(Ordering::Relaxed) < cfg.msgs {
        std::hint::spin_loop();
    }

    handle.shutdown();
    latency_stats_from_nanos(rec.collect())
}

fn bench_latency_big1024_tell_with(cfg: Config) -> _bench_util::LatencyStats {
    let sample_every = cfg.sample_every.max(1);
    let samples_cap = (cfg.msgs / sample_every).max(1) as usize + 64;
    let rec = Arc::new(LatencyBuf::new(samples_cap));

    let processed = Arc::new(AtomicU64::new(0));
    let processed_w = Arc::clone(&processed);

    let base = Instant::now();
    let base_w = base;
    let rec_w = Arc::clone(&rec);

    let (addr, handle) = local_sync::mpsc::spawn(cfg.cap, move |msg: Big1024| {
        if let Some(t0) = big1024_get_t0(&msg) {
            let now = now_ns(base_w);
            rec_w.record(now.saturating_sub(t0));
        }
        std::hint::black_box(msg.buf[8]);
        processed_w.fetch_add(1, Ordering::Relaxed);
    });

    let payload = [3u8; 1024];

    for _ in 0..cfg.warmup {
        let mut spins = 0u32;
        loop {
            match addr.tell_with(|p: *mut Big1024| unsafe {
                std::ptr::copy_nonoverlapping(payload.as_ptr(), (*p).buf.as_mut_ptr(), 1024);
                (&mut (*p).buf)[0..8].copy_from_slice(&0u64.to_le_bytes());
            }) {
                Ok(()) => break,
                Err(()) => spin_then_yield(&mut spins),
            }
        }
    }
    while processed.load(Ordering::Relaxed) < cfg.warmup {
        std::hint::spin_loop();
    }
    processed.store(0, Ordering::Relaxed);
    rec.reset();

    let mut rl = RateLimiter::new(base, cfg.rate);
    for i in 0..cfg.msgs {
        rl.wait();
        let t0 = if i % sample_every == 0 {
            encode_sample_t0(now_ns(base))
        } else {
            0
        };

        let mut spins = 0u32;
        loop {
            match addr.tell_with(|p: *mut Big1024| unsafe {
                std::ptr::copy_nonoverlapping(payload.as_ptr(), (*p).buf.as_mut_ptr(), 1024);
                (&mut (*p).buf)[0..8].copy_from_slice(&t0.to_le_bytes());
            }) {
                Ok(()) => break,
                Err(()) => spin_then_yield(&mut spins),
            }
        }
    }

    while processed.load(Ordering::Relaxed) < cfg.msgs {
        std::hint::spin_loop();
    }

    handle.shutdown();
    latency_stats_from_nanos(rec.collect())
}

fn want(only: &Option<String>, v: &str) -> bool {
    only.as_ref().map_or(true, |o| o == v)
}

fn main() {
    let (cfg, only) = match parse_config() {
        Ok(v) => v,
        Err(msg) => {
            eprintln!("{msg}");
            return;
        }
    };

    #[cfg(feature = "perf-counters")]
    local_sync::counters_reset();

    println!(
        "config: msgs={} warmup={} cap={} rate={} sample_every={}",
        cfg.msgs, cfg.warmup, cfg.cap, cfg.rate, cfg.sample_every
    );
    println!();

    if want(&only, "u64") {
        println!("=== LATENCY (u64) ===\n");
        alloc_reset();
        let s = bench_latency_u64(cfg);
        let alloc = alloc_snapshot();
        print_latency_row("af::Addr<u64>::tell (cross-thread)", &s);
        print_alloc_stats_row("latency_u64", cfg.msgs, &alloc);
    }
    if want(&only, "u64_tell_with") {
        println!("=== LATENCY (u64) ===\n");
        alloc_reset();
        let s = bench_latency_u64_tell_with(cfg);
        let alloc = alloc_snapshot();
        print_latency_row("af::Addr<u64>::tell_with (cross-thread)", &s);
        print_alloc_stats_row("latency_u64_tell_with", cfg.msgs, &alloc);
    }
    if want(&only, "a2a_u64") {
        println!("=== LATENCY (u64) ===\n");
        alloc_reset();
        let s = bench_latency_actor_to_actor_u64(cfg);
        let alloc = alloc_snapshot();
        print_latency_row("af::actor->actor tell (cross-thread)", &s);
        print_alloc_stats_row("latency_a2a_u64", cfg.msgs, &alloc);
    }
    if want(&only, "s128") {
        println!("=== LATENCY (struct: S128) ===\n");
        alloc_reset();
        let s = bench_latency_s128(cfg);
        let alloc = alloc_snapshot();
        print_latency_row("af::Addr<S128>::tell (cross-thread)", &s);
        print_alloc_stats_row("latency_s128", cfg.msgs, &alloc);
    }
    if want(&only, "big1024") {
        println!("=== LATENCY (large payload: Big1024) ===\n");
        alloc_reset();
        let s = bench_latency_big1024(cfg);
        let alloc = alloc_snapshot();
        print_latency_row("af::Addr<Big1024>::tell (cross-thread)", &s);
        print_alloc_stats_row("latency_big1024", cfg.msgs, &alloc);
    }
    if want(&only, "big1024_tell_with") {
        println!("=== LATENCY (large payload: Big1024) ===\n");
        alloc_reset();
        let s = bench_latency_big1024_tell_with(cfg);
        let alloc = alloc_snapshot();
        print_latency_row("af::Addr<Big1024>::tell_with (cross-thread)", &s);
        print_alloc_stats_row("latency_big1024_tell_with", cfg.msgs, &alloc);
    }

    #[cfg(feature = "perf-counters")]
    {
        let counters = local_sync::counters_snapshot();
        println!("perf_counters_enabled: {}", local_sync::COUNTERS_ENABLED);
        println!("perf_counters.mpsc: {:?}", counters.mpsc);
        println!("perf_counters.ask: {:?}", counters.ask);
    }
}
