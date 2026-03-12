use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use icanact_core::{Actor, local_sync as sync};
use std::{
    env,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

#[derive(Clone, Debug)]
struct Config {
    msgs: u64,
    warmup_msgs: u64,
    caps: Vec<usize>,
    producers: Vec<usize>,
    sample_size: usize,
    warmup_ms: u64,
    measurement_ms: u64,
}

fn parse_u64_env(key: &str, default: u64) -> u64 {
    match env::var(key) {
        Ok(v) => v.parse::<u64>().unwrap_or(default),
        Err(_) => default,
    }
}

fn parse_usize_env(key: &str, default: usize) -> usize {
    match env::var(key) {
        Ok(v) => v.parse::<usize>().unwrap_or(default),
        Err(_) => default,
    }
}

fn parse_list_env(key: &str, default: &[usize]) -> Vec<usize> {
    match env::var(key) {
        Ok(v) => {
            let mut out = Vec::new();
            for tok in v.split(',') {
                let t = tok.trim();
                if t.is_empty() {
                    continue;
                }
                if let Ok(n) = t.parse::<usize>()
                    && n > 0
                {
                    out.push(n);
                }
            }
            if out.is_empty() {
                default.to_vec()
            } else {
                out
            }
        }
        Err(_) => default.to_vec(),
    }
}

impl Config {
    fn from_env() -> Self {
        let sample_size = parse_usize_env("AF_CRIT_SAMPLE_SIZE", 20).clamp(10, 200);
        Self {
            msgs: parse_u64_env("AF_CRIT_MSGS", 10_000_000),
            warmup_msgs: parse_u64_env("AF_CRIT_WARMUP_MSGS", 50_000),
            caps: parse_list_env("AF_CRIT_CAPS", &[65_536]),
            producers: parse_list_env("AF_CRIT_PRODUCERS", &[1, 2]),
            sample_size,
            warmup_ms: parse_u64_env("AF_CRIT_WARMUP_MS", 1000),
            measurement_ms: parse_u64_env("AF_CRIT_MEASUREMENT_MS", 3000),
        }
    }
}

struct BenchActor {
    processed: Arc<AtomicU64>,
}

impl Actor for BenchActor {
    type Msg = u64;

    #[inline(always)]
    fn handle_tell(&mut self, _msg: Self::Msg) {
        self.processed.fetch_add(1, Ordering::Relaxed);
    }
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

fn bench_local_tell(c: &mut Criterion) {
    let cfg = Config::from_env();

    eprintln!(
        "local_tell_criterion config: msgs={} warmup_msgs={} caps={:?} producers={:?} sample_size={} warmup_ms={} measurement_ms={}",
        cfg.msgs,
        cfg.warmup_msgs,
        cfg.caps,
        cfg.producers,
        cfg.sample_size,
        cfg.warmup_ms,
        cfg.measurement_ms
    );

    let mut group = c.benchmark_group("local_tell_e2e");
    group.sample_size(cfg.sample_size);
    group.warm_up_time(Duration::from_millis(cfg.warmup_ms.max(100)));
    group.measurement_time(Duration::from_millis(cfg.measurement_ms.max(1000)));

    for &cap in &cfg.caps {
        for &producers in &cfg.producers {
            let processed = Arc::new(AtomicU64::new(0));
            let (addr, handle) = sync::spawn_actor(
                cap,
                BenchActor {
                    processed: Arc::clone(&processed),
                },
            );
            handle.wait_for_startup();

            run_producers(&addr, cfg.warmup_msgs, producers);
            while processed.load(Ordering::Relaxed) < cfg.warmup_msgs {
                std::hint::spin_loop();
            }

            let msgs = cfg.msgs;
            let id = BenchmarkId::new("tell", format!("cap={cap},producers={producers}"));
            group.throughput(Throughput::Elements(msgs));

            let addr_bench = addr.clone();
            let processed_bench = Arc::clone(&processed);
            group.bench_with_input(id, &(cap, producers), move |b, _input| {
                b.iter_custom(|iters| {
                    let t0 = Instant::now();
                    for _ in 0..iters {
                        processed_bench.store(0, Ordering::Relaxed);
                        run_producers(&addr_bench, msgs, producers);
                        while processed_bench.load(Ordering::Relaxed) < msgs {
                            std::hint::spin_loop();
                        }
                    }
                    t0.elapsed()
                });
            });

            handle.shutdown();
        }
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().configure_from_args();
    targets = bench_local_tell
}
criterion_main!(benches);
