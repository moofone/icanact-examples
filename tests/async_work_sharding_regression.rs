#![cfg(feature = "async-actors")]

use icanact_core::{local_async, local_async::AsyncActor};
use ractor::{
    Actor as RactorActor, ActorProcessingErr as RactorActorProcessingErr,
    ActorRef as RactorActorRef,
};
use std::future::Future;
use std::hint::black_box;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

#[inline(always)]
fn run_micro_compute(iters: u32) -> u64 {
    let mut x = 0x9E37_79B9_7F4A_7C15u64;
    for i in 0..iters {
        x = x
            .wrapping_mul(6364136223846793005u64)
            .wrapping_add((i as u64).wrapping_add(1442695040888963407u64))
            ^ (x >> 29);
    }
    x
}

struct AfAsyncWorkActor {
    completed: Arc<AtomicU64>,
    iters: u32,
}

impl AsyncActor for AfAsyncWorkActor {
    type Msg = ();

    #[inline(always)]
    fn handle_tell(&mut self, _msg: Self::Msg) -> impl Future<Output = ()> + Send {
        black_box(run_micro_compute(self.iters));
        self.completed.fetch_add(1, Ordering::Release);
        std::future::ready(())
    }
}

struct RactorAsyncWorkActor;

impl RactorActor for RactorAsyncWorkActor {
    type Msg = ();
    type State = (Arc<AtomicU64>, u32);
    type Arguments = (Arc<AtomicU64>, u32);

    #[inline(always)]
    fn pre_start(
        &self,
        _myself: RactorActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> impl Future<Output = Result<Self::State, RactorActorProcessingErr>> + Send {
        std::future::ready(Ok(args))
    }

    #[inline(always)]
    fn handle(
        &self,
        _myself: RactorActorRef<Self::Msg>,
        _message: Self::Msg,
        state: &mut Self::State,
    ) -> impl Future<Output = Result<(), RactorActorProcessingErr>> + Send {
        let (completed, iters) = state;
        black_box(run_micro_compute(*iters));
        completed.fetch_add(1, Ordering::Release);
        std::future::ready(Ok(()))
    }
}

fn parse_u64_env(key: &str, default: u64) -> u64 {
    match std::env::var(key) {
        Ok(v) => v.parse::<u64>().unwrap_or(default),
        Err(_) => default,
    }
}

fn parse_usize_env(key: &str, default: usize) -> usize {
    match std::env::var(key) {
        Ok(v) => v.parse::<usize>().unwrap_or(default),
        Err(_) => default,
    }
}

fn parse_f64_env(key: &str, default: f64) -> f64 {
    match std::env::var(key) {
        Ok(v) => v.parse::<f64>().unwrap_or(default),
        Err(_) => default,
    }
}

fn median(mut vals: Vec<f64>) -> f64 {
    vals.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    vals[vals.len() / 2]
}

fn throughput_melems(ops: u64, elapsed: Duration) -> f64 {
    (ops as f64) / elapsed.as_secs_f64() / 1_000_000.0
}

#[inline(always)]
fn sum_completed(counters: &[Arc<AtomicU64>]) -> u64 {
    counters
        .iter()
        .map(|counter| counter.load(Ordering::Acquire))
        .sum::<u64>()
}

#[derive(Clone, Copy, Debug)]
struct ScalingSnapshot {
    af1: f64,
    af4: f64,
    r1: f64,
    r4: f64,
}

impl ScalingSnapshot {
    #[inline(always)]
    fn af_speedup(self) -> f64 {
        self.af4 / self.af1
    }

    #[inline(always)]
    fn r_speedup(self) -> f64 {
        self.r4 / self.r1
    }

    #[inline(always)]
    fn af_vs_r_speedup_ratio(self) -> f64 {
        self.af_speedup() / self.r_speedup()
    }

    #[inline(always)]
    fn af4_vs_r4_ratio(self) -> f64 {
        self.af4 / self.r4
    }
}

fn run_af_async_work_sharded(
    ops: u64,
    work_iters: u32,
    shards: usize,
    mailbox_cap: usize,
) -> Duration {
    let shards = shards.max(1);
    let cap = mailbox_cap.max(1);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_time()
        .build()
        .expect("failed to build tokio runtime");

    rt.block_on(async move {
        let mut completed = Vec::with_capacity(shards);
        let mut addrs = Vec::with_capacity(shards);
        let mut handles = Vec::with_capacity(shards);
        for _ in 0..shards {
            let shard_completed = Arc::new(AtomicU64::new(0));
            let (addr, handle) = local_async::spawn_actor(
                cap,
                AfAsyncWorkActor {
                    completed: Arc::clone(&shard_completed),
                    iters: work_iters,
                },
            );
            completed.push(shard_completed);
            addrs.push(addr);
            handles.push(handle);
        }

        let warmup = ops.min(10_000);
        let mut rr = 0usize;
        for _ in 0..warmup {
            loop {
                match addrs[rr].try_tell(()) {
                    Ok(()) => break,
                    Err(local_async::TryTellError::Full(())) => tokio::task::yield_now().await,
                    Err(local_async::TryTellError::Closed(())) => {
                        panic!("actor-framework async work actor closed during warmup")
                    }
                }
            }
            rr += 1;
            if rr == shards {
                rr = 0;
            }
        }
        while sum_completed(&completed) < warmup {
            tokio::task::yield_now().await;
        }

        for counter in &completed {
            counter.store(0, Ordering::Release);
        }
        rr = 0;
        let t0 = Instant::now();
        for _ in 0..ops {
            loop {
                match addrs[rr].try_tell(()) {
                    Ok(()) => break,
                    Err(local_async::TryTellError::Full(())) => tokio::task::yield_now().await,
                    Err(local_async::TryTellError::Closed(())) => {
                        panic!("actor-framework async work actor closed")
                    }
                }
            }
            rr += 1;
            if rr == shards {
                rr = 0;
            }
        }
        while sum_completed(&completed) < ops {
            tokio::task::yield_now().await;
        }
        let elapsed = t0.elapsed();

        for handle in handles {
            handle.shutdown().await;
        }
        elapsed
    })
}

fn run_ractor_async_work_sharded(ops: u64, work_iters: u32, shards: usize) -> Duration {
    let shards = shards.max(1);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_time()
        .build()
        .expect("failed to build tokio runtime");

    rt.block_on(async move {
        let mut completed = Vec::with_capacity(shards);
        let mut addrs = Vec::with_capacity(shards);
        let mut joins = Vec::with_capacity(shards);
        for _ in 0..shards {
            let shard_completed = Arc::new(AtomicU64::new(0));
            let (addr, join) = RactorAsyncWorkActor::spawn(
                None,
                RactorAsyncWorkActor,
                (Arc::clone(&shard_completed), work_iters),
            )
            .await
            .expect("failed to spawn ractor async work actor");
            completed.push(shard_completed);
            addrs.push(addr);
            joins.push(join);
        }

        let warmup = ops.min(10_000);
        let mut rr = 0usize;
        for _ in 0..warmup {
            addrs[rr]
                .send_message(())
                .expect("ractor async work warmup send failed");
            rr += 1;
            if rr == shards {
                rr = 0;
            }
        }
        while sum_completed(&completed) < warmup {
            tokio::task::yield_now().await;
        }

        for counter in &completed {
            counter.store(0, Ordering::Release);
        }
        rr = 0;
        let t0 = Instant::now();
        for _ in 0..ops {
            addrs[rr]
                .send_message(())
                .expect("ractor async work send failed");
            rr += 1;
            if rr == shards {
                rr = 0;
            }
        }
        while sum_completed(&completed) < ops {
            tokio::task::yield_now().await;
        }
        let elapsed = t0.elapsed();

        for addr in addrs {
            addr.stop(None);
        }
        for join in joins {
            join.await.expect("ractor async work join failed");
        }
        elapsed
    })
}

fn sample_scaling_snapshot(
    ops: u64,
    work_iters: u32,
    samples: usize,
    mailbox_cap: usize,
) -> ScalingSnapshot {
    let mut af1 = Vec::with_capacity(samples);
    let mut af4 = Vec::with_capacity(samples);
    let mut r1 = Vec::with_capacity(samples);
    let mut r4 = Vec::with_capacity(samples);

    for _ in 0..samples {
        let af_1x = throughput_melems(
            ops,
            run_af_async_work_sharded(ops, work_iters, 1, mailbox_cap),
        );
        let af_4x = throughput_melems(
            ops,
            run_af_async_work_sharded(ops, work_iters, 4, mailbox_cap),
        );
        let r_1x = throughput_melems(ops, run_ractor_async_work_sharded(ops, work_iters, 1));
        let r_4x = throughput_melems(ops, run_ractor_async_work_sharded(ops, work_iters, 4));
        af1.push(af_1x);
        af4.push(af_4x);
        r1.push(r_1x);
        r4.push(r_4x);
    }

    ScalingSnapshot {
        af1: median(af1),
        af4: median(af4),
        r1: median(r1),
        r4: median(r4),
    }
}

#[test]
fn async_work_sharding_scaling_regression_guard() {
    let ops = parse_u64_env("AF_GUARD_OPS", 120_000).max(10_000);
    let work_iters = u32::try_from(parse_u64_env("AF_GUARD_WORK_ITERS", 256))
        .unwrap_or(256)
        .max(1);
    let low_work_iters = u32::try_from(parse_u64_env("AF_GUARD_LOW_WORK_ITERS", 64))
        .unwrap_or(64)
        .max(1);
    let samples = parse_usize_env("AF_GUARD_SAMPLES", 5).clamp(3, 11);
    let mailbox_cap = parse_usize_env("AF_GUARD_ASYNC_MAILBOX_CAP", 300_000).max(1);

    let min_af_speedup = parse_f64_env("AF_GUARD_MIN_AF_SPEEDUP", 2.20);
    let min_af_vs_ractor_speedup_ratio =
        parse_f64_env("AF_GUARD_MIN_AF_TO_RACTOR_SPEEDUP_RATIO", 0.75);
    let min_af4_vs_ractor4_ratio = parse_f64_env("AF_GUARD_MIN_AF4_TO_RACTOR4_RATIO", 1.05);
    let min_low_af_speedup = parse_f64_env("AF_GUARD_MIN_LOW_AF_SPEEDUP", 1.10);

    let base = sample_scaling_snapshot(ops, work_iters, samples, mailbox_cap);
    let low = sample_scaling_snapshot(ops, low_work_iters, samples, mailbox_cap);

    println!(
        "async-sharding-guard: ops={}, work_iters={}, samples={}, mailbox_cap={}",
        ops, work_iters, samples, mailbox_cap
    );
    println!(
        "async-sharding-guard: af={{1x:{:.3},4x:{:.3},speedup:{:.3}x}} ractor={{1x:{:.3},4x:{:.3},speedup:{:.3}x}}",
        base.af1,
        base.af4,
        base.af_speedup(),
        base.r1,
        base.r4,
        base.r_speedup()
    );
    println!(
        "async-sharding-guard: af_speedup/ractor_speedup={:.3}x af4/ractor4={:.3}x",
        base.af_vs_r_speedup_ratio(),
        base.af4_vs_r4_ratio()
    );
    println!(
        "async-sharding-guard (low-work): work_iters={}, af={{1x:{:.3},4x:{:.3},speedup:{:.3}x}} ractor={{1x:{:.3},4x:{:.3},speedup:{:.3}x}}",
        low_work_iters,
        low.af1,
        low.af4,
        low.af_speedup(),
        low.r1,
        low.r4,
        low.r_speedup()
    );

    assert!(
        base.af_speedup() >= min_af_speedup,
        "AF async sharding speedup regression: {:.3}x < min {:.3}x",
        base.af_speedup(),
        min_af_speedup
    );
    assert!(
        base.af_vs_r_speedup_ratio() >= min_af_vs_ractor_speedup_ratio,
        "AF async sharding relative speedup regression: AF/Ractor speedup ratio {:.3}x < min {:.3}x",
        base.af_vs_r_speedup_ratio(),
        min_af_vs_ractor_speedup_ratio
    );
    assert!(
        base.af4_vs_r4_ratio() >= min_af4_vs_ractor4_ratio,
        "AF async 4x throughput regression vs Ractor: {:.3}x < min {:.3}x",
        base.af4_vs_r4_ratio(),
        min_af4_vs_ractor4_ratio
    );
    assert!(
        low.af_speedup() >= min_low_af_speedup,
        "AF low-work async sharding speedup regression: {:.3}x < min {:.3}x",
        low.af_speedup(),
        min_low_af_speedup
    );
}
