use icanact_core::{Actor as AfActor, local_direct as direct, local_sync as sync};
use std::{
    env,
    hint::black_box,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

const GO_THRESHOLD_RATIO: f64 = 1.30;

#[derive(Clone, Debug)]
struct Config {
    ops: u64,
    warmup: u64,
    samples: usize,
    cap: usize,
    producers: Vec<usize>,
}

fn usage() -> &'static str {
    "Usage: cargo bench -p icanact-core --bench ephemeral_slacktor_poc -- [options]\n\
     \n\
     Options:\n\
       --ops N          Ops per producer per sample (default: AF_CMP_EPHEMERAL_OPS or 1000)\n\
       --warmup N       Warmup ops per producer before each sample (default: min(ops, 256), min 2)\n\
       --samples N      Samples per lane/scenario (default: AF_POC_SAMPLES or 9)\n\
       --cap N          AF mailbox capacity (default: AF_CMP_SPAWN_MAILBOX_CAP or 64)\n\
       --producers CSV  Producer list, e.g. 1,4 (default: AF_POC_PRODUCERS or 1,4)\n"
}

fn parse_u64(s: &str) -> Result<u64, String> {
    s.parse::<u64>().map_err(|e| e.to_string())
}

fn parse_usize(s: &str) -> Result<usize, String> {
    s.parse::<usize>().map_err(|e| e.to_string())
}

fn parse_producers(raw: &str) -> Result<Vec<usize>, String> {
    let mut out = Vec::new();
    for p in raw.split(',') {
        let trimmed = p.trim();
        if trimmed.is_empty() {
            continue;
        }
        let n = parse_usize(trimmed)?;
        if n == 0 {
            return Err("producer count must be >= 1".to_string());
        }
        out.push(n);
    }
    out.sort_unstable();
    out.dedup();
    if out.is_empty() {
        return Err("producer list cannot be empty".to_string());
    }
    Ok(out)
}

fn parse_u64_env(key: &str) -> Result<Option<u64>, String> {
    match env::var(key) {
        Ok(v) => Ok(Some(parse_u64(&v)?)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(e) => Err(e.to_string()),
    }
}

fn parse_usize_env(key: &str) -> Result<Option<usize>, String> {
    match env::var(key) {
        Ok(v) => Ok(Some(parse_usize(&v)?)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(e) => Err(e.to_string()),
    }
}

fn parse_producers_env(key: &str) -> Result<Option<Vec<usize>>, String> {
    match env::var(key) {
        Ok(v) => Ok(Some(parse_producers(&v)?)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(e) => Err(e.to_string()),
    }
}

fn parse_config() -> Result<Config, String> {
    let mut cfg = Config {
        ops: 1_000,
        warmup: 256,
        samples: 9,
        cap: 64,
        producers: vec![1, 4],
    };

    if let Some(v) = parse_u64_env("AF_CMP_EPHEMERAL_OPS")? {
        cfg.ops = v.max(1);
    }
    if let Some(v) = parse_usize_env("AF_CMP_SPAWN_MAILBOX_CAP")? {
        cfg.cap = v.max(1);
    }
    if let Some(v) = parse_u64_env("AF_POC_WARMUP")? {
        cfg.warmup = v;
    } else {
        cfg.warmup = cfg.ops.min(256);
    }
    if let Some(v) = parse_usize_env("AF_POC_SAMPLES")? {
        cfg.samples = v.max(1);
    }
    if let Some(v) = parse_producers_env("AF_POC_PRODUCERS")? {
        cfg.producers = v;
    }

    let mut it = env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--bench" => {}
            "--help" | "-h" => return Err(usage().to_string()),
            "--ops" => {
                cfg.ops = it
                    .next()
                    .ok_or_else(|| "missing value for --ops".to_string())
                    .and_then(|s| parse_u64(&s))?
                    .max(1);
            }
            "--warmup" => {
                cfg.warmup = it
                    .next()
                    .ok_or_else(|| "missing value for --warmup".to_string())
                    .and_then(|s| parse_u64(&s))?;
            }
            "--samples" => {
                cfg.samples = it
                    .next()
                    .ok_or_else(|| "missing value for --samples".to_string())
                    .and_then(|s| parse_usize(&s))?
                    .max(1);
            }
            "--cap" => {
                cfg.cap = it
                    .next()
                    .ok_or_else(|| "missing value for --cap".to_string())
                    .and_then(|s| parse_usize(&s))?
                    .max(1);
            }
            "--producers" => {
                cfg.producers = it
                    .next()
                    .ok_or_else(|| "missing value for --producers".to_string())
                    .and_then(|s| parse_producers(&s))?;
            }
            other => return Err(format!("unknown arg: {other}\n\n{}", usage())),
        }
    }

    cfg.warmup = cfg.warmup.max(2);
    Ok(cfg)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Workflow {
    Compute,
    Ask,
}

impl Workflow {
    fn as_str(self) -> &'static str {
        match self {
            Workflow::Compute => "compute",
            Workflow::Ask => "ask",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Lane {
    AfEphemeralCompute,
    AfEphemeralAsk,
    DirectReuseCompute,
    DirectReuseAsk,
    DirectRespawnCompute,
    DirectRespawnAsk,
}

impl Lane {
    fn as_str(self) -> &'static str {
        match self {
            Lane::AfEphemeralCompute => "af_ephemeral_compute",
            Lane::AfEphemeralAsk => "af_ephemeral_ask",
            Lane::DirectReuseCompute => "direct_reuse_compute",
            Lane::DirectReuseAsk => "direct_reuse_ask",
            Lane::DirectRespawnCompute => "direct_respawn_compute",
            Lane::DirectRespawnAsk => "direct_respawn_ask",
        }
    }

    fn mode(self) -> &'static str {
        match self {
            Lane::AfEphemeralCompute | Lane::AfEphemeralAsk => "respawn(spawn_reusable)",
            Lane::DirectReuseCompute | Lane::DirectReuseAsk => "reuse",
            Lane::DirectRespawnCompute | Lane::DirectRespawnAsk => "respawn(new-session)",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Scenario {
    workflow: Workflow,
    producers: usize,
}

impl Scenario {
    fn label(self) -> String {
        format!(
            "workflow={} payload=u64 producers={}",
            self.workflow.as_str(),
            self.producers
        )
    }
}

const COMPUTE_LANES: [Lane; 3] = [
    Lane::AfEphemeralCompute,
    Lane::DirectReuseCompute,
    Lane::DirectRespawnCompute,
];

const ASK_LANES: [Lane; 3] = [
    Lane::AfEphemeralAsk,
    Lane::DirectReuseAsk,
    Lane::DirectRespawnAsk,
];

fn lanes_for_workflow(workflow: Workflow) -> &'static [Lane] {
    match workflow {
        Workflow::Compute => &COMPUTE_LANES,
        Workflow::Ask => &ASK_LANES,
    }
}

fn baseline_lane_for_workflow(workflow: Workflow) -> Lane {
    match workflow {
        Workflow::Compute => Lane::AfEphemeralCompute,
        Workflow::Ask => Lane::AfEphemeralAsk,
    }
}

#[derive(Clone, Copy, Debug)]
struct LaneStats {
    lane: Lane,
    median_ops_per_sec: f64,
    median_ns_per_op: f64,
    total_ops: u64,
}

#[derive(Clone, Debug)]
struct ScenarioResult {
    scenario: Scenario,
    lanes: Vec<LaneStats>,
    ratios: Vec<(Lane, f64)>,
}

fn spin_then_yield(spins: &mut u32) {
    if *spins < 64 {
        *spins += 1;
        std::hint::spin_loop();
    } else {
        *spins = 0;
        thread::yield_now();
    }
}

fn wait_for_start_signal(start: &AtomicBool) {
    let mut spins = 0u32;
    while !start.load(Ordering::Acquire) {
        spin_then_yield(&mut spins);
    }
}

fn panic_payload_to_string(p: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = p.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = p.downcast_ref::<String>() {
        s.clone()
    } else {
        "panic (non-string payload)".to_string()
    }
}

fn median_duration(samples: &[Duration]) -> Duration {
    assert!(!samples.is_empty(), "median requires at least one sample");
    let mut v = samples.to_vec();
    v.sort_unstable_by_key(|d| d.as_nanos());
    v[v.len() / 2]
}

fn run_workers<F>(producers: usize, worker: F) -> Duration
where
    F: Fn(usize, mpsc::Sender<()>, Arc<AtomicBool>) + Send + Sync + 'static,
{
    let (ready_tx, ready_rx) = mpsc::channel::<()>();
    let start = Arc::new(AtomicBool::new(false));
    let worker = Arc::new(worker);
    let mut joins = Vec::with_capacity(producers);

    for worker_idx in 0..producers {
        let tx = ready_tx.clone();
        let start2 = Arc::clone(&start);
        let worker2 = Arc::clone(&worker);
        joins.push(thread::spawn(move || worker2(worker_idx, tx, start2)));
    }
    drop(ready_tx);

    for _ in 0..producers {
        if let Err(e) = ready_rx.recv_timeout(Duration::from_secs(30)) {
            for j in joins {
                if let Err(p) = j.join() {
                    panic!(
                        "worker panicked before ready: {}",
                        panic_payload_to_string(&*p)
                    );
                }
            }
            panic!("worker did not reach ready state within 30s: {e}");
        }
    }

    let t0 = Instant::now();
    start.store(true, Ordering::Release);

    for j in joins {
        if let Err(p) = j.join() {
            panic!("worker panicked: {}", panic_payload_to_string(&*p));
        }
    }

    t0.elapsed()
}

struct AfEphemeralComputeActor {
    processed: Arc<AtomicU64>,
    seen_generation: Arc<AtomicU64>,
    generation: u64,
}

impl AfActor for AfEphemeralComputeActor {
    type Msg = u64;

    #[inline(always)]
    fn handle_tell(&mut self, msg: Self::Msg) {
        self.seen_generation
            .store(self.generation, Ordering::Release);
        self.processed.store(black_box(msg), Ordering::Release);
    }
}

#[derive(Clone, Copy, Debug)]
struct AskResp {
    value: u64,
    generation: u64,
}

enum AfEphemeralAskMsg {
    Compute {
        token: u64,
        reply: sync::ReplyTo<AskResp>,
    },
}

struct AfEphemeralAskActor {
    generation: u64,
}

impl AfActor for AfEphemeralAskActor {
    type Msg = AfEphemeralAskMsg;

    #[inline(always)]
    fn handle_tell(&mut self, msg: Self::Msg) {
        match msg {
            AfEphemeralAskMsg::Compute { token, reply } => {
                let _ = reply.reply(AskResp {
                    value: token.wrapping_add(1),
                    generation: self.generation,
                });
            }
        }
    }
}

enum DirectAskMsg {
    Compute {
        token: u64,
        reply: sync::ReplyTo<AskResp>,
    },
}

struct DirectAskActor {
    generation: u64,
}

impl AfActor for DirectAskActor {
    type Msg = DirectAskMsg;
    #[inline(always)]
    fn handle_tell(&mut self, msg: Self::Msg) {
        match msg {
            DirectAskMsg::Compute { token, reply } => {
                let _ = reply.reply(AskResp {
                    value: black_box(token).wrapping_add(1),
                    generation: self.generation,
                });
            }
        }
    }
}

fn af_compute_round(
    addr: &sync::MailboxAddr<u64>,
    handle: &sync::ReusableActorHandle,
    processed: &AtomicU64,
    seen_generation: &AtomicU64,
    token: u64,
    prev_generation: &mut Option<u64>,
    phase: &str,
) {
    let lane = Lane::AfEphemeralCompute.as_str();
    processed.store(0, Ordering::Release);
    let expected = black_box(token);
    let mut pending = expected;
    let mut spins = 0u32;
    loop {
        match addr.try_tell(pending) {
            Ok(()) => break,
            Err(msg) => {
                pending = msg;
                spin_then_yield(&mut spins);
            }
        }
    }
    while processed.load(Ordering::Acquire) != expected {
        spin_then_yield(&mut spins);
    }
    let generation = seen_generation.load(Ordering::Acquire);
    if let Some(prev) = *prev_generation {
        assert!(
            generation != prev,
            "{lane} {phase}: expected fresh actor generation, prev={prev} current={generation}"
        );
    }
    *prev_generation = Some(generation);
    assert!(
        handle.restart_and_wait(),
        "{lane} {phase}: restart_and_wait failed"
    );
}

fn af_ask_round(
    addr: &sync::MailboxAddr<AfEphemeralAskMsg>,
    handle: &sync::ReusableActorHandle,
    token: u64,
    prev_generation: &mut Option<u64>,
    phase: &str,
) {
    let lane = Lane::AfEphemeralAsk.as_str();
    let expected_token = black_box(token);
    let resp = sync::ask(addr, |reply| AfEphemeralAskMsg::Compute {
        token: expected_token,
        reply,
    })
    .expect("af ask failed");
    let expected_value = expected_token.wrapping_add(1);
    assert!(
        resp.value == expected_value,
        "{lane} {phase}: ask mismatch: got={} expected={expected_value}",
        resp.value
    );
    if let Some(prev) = *prev_generation {
        assert!(
            resp.generation != prev,
            "{lane} {phase}: expected fresh actor generation, prev={prev} current={}",
            resp.generation
        );
    }
    *prev_generation = Some(resp.generation);
    assert!(
        handle.restart_and_wait(),
        "{lane} {phase}: restart_and_wait failed"
    );
}

fn direct_reuse_compute_round(
    actor: &mut direct::Actor<AfEphemeralComputeActor>,
    processed: &AtomicU64,
    seen_generation: &AtomicU64,
    token: u64,
    pinned_generation: &mut Option<u64>,
    phase: &str,
) {
    let lane = Lane::DirectReuseCompute.as_str();
    processed.store(0, Ordering::Release);
    let expected = black_box(token);
    actor
        .tell(expected)
        .expect("direct reuse compute tell failed");
    let got = processed.load(Ordering::Acquire);
    assert!(
        got == expected,
        "{lane} {phase}: compute mismatch: got={got} expected={expected}"
    );
    let generation = seen_generation.load(Ordering::Acquire);
    if let Some(pinned) = *pinned_generation {
        assert!(
            generation == pinned,
            "{lane} {phase}: expected reused actor generation, pinned={pinned} current={generation}"
        );
    } else {
        *pinned_generation = Some(generation);
    }
}

fn direct_reuse_ask_round(
    actor: &mut direct::Actor<DirectAskActor>,
    token: u64,
    pinned_generation: &mut Option<u64>,
    phase: &str,
) {
    let lane = Lane::DirectReuseAsk.as_str();
    let expected_token = black_box(token);
    let resp = actor
        .ask(|reply| DirectAskMsg::Compute {
            token: expected_token,
            reply,
        })
        .expect("direct reuse ask failed");
    let expected_value = expected_token.wrapping_add(1);
    assert!(
        resp.value == expected_value,
        "{lane} {phase}: ask mismatch: got={} expected={expected_value}",
        resp.value
    );
    if let Some(pinned) = *pinned_generation {
        assert!(
            resp.generation == pinned,
            "{lane} {phase}: expected reused actor generation, pinned={pinned} current={}",
            resp.generation
        );
    } else {
        *pinned_generation = Some(resp.generation);
    }
}

fn direct_respawn_compute_round(
    processed: &Arc<AtomicU64>,
    seen_generation: &Arc<AtomicU64>,
    token: u64,
    next_generation: &mut u64,
    prev_generation: &mut Option<u64>,
    phase: &str,
) {
    let lane = Lane::DirectRespawnCompute.as_str();
    processed.store(0, Ordering::Release);
    let generation = *next_generation;
    *next_generation = next_generation.wrapping_add(1);

    let processed2 = Arc::clone(processed);
    let seen2 = Arc::clone(seen_generation);
    let mut actor = direct::Actor::new(move || AfEphemeralComputeActor {
        processed: Arc::clone(&processed2),
        seen_generation: Arc::clone(&seen2),
        generation,
    });

    let expected = black_box(token);
    actor
        .tell(expected)
        .expect("direct respawn compute tell failed");
    let got = processed.load(Ordering::Acquire);
    let got_generation = seen_generation.load(Ordering::Acquire);
    assert!(
        got == expected,
        "{lane} {phase}: compute mismatch: got={got} expected={expected}"
    );
    assert!(
        got_generation == generation,
        "{lane} {phase}: generation mismatch: got={got_generation} expected={generation}"
    );
    if let Some(prev) = *prev_generation {
        assert!(
            generation != prev,
            "{lane} {phase}: expected fresh actor generation, prev={prev} current={generation}"
        );
    }
    *prev_generation = Some(generation);
    actor.shutdown();
}

fn direct_respawn_ask_round(
    token: u64,
    next_generation: &mut u64,
    prev_generation: &mut Option<u64>,
    phase: &str,
) {
    let lane = Lane::DirectRespawnAsk.as_str();
    let generation = *next_generation;
    *next_generation = next_generation.wrapping_add(1);

    let mut actor = direct::Actor::new(move || DirectAskActor { generation });
    let expected_token = black_box(token);
    let resp = actor
        .ask(|reply| DirectAskMsg::Compute {
            token: expected_token,
            reply,
        })
        .expect("direct respawn ask failed");
    let expected_value = expected_token.wrapping_add(1);
    assert!(
        resp.value == expected_value,
        "{lane} {phase}: ask mismatch: got={} expected={expected_value}",
        resp.value
    );
    assert!(
        resp.generation == generation,
        "{lane} {phase}: generation mismatch: got={} expected={generation}",
        resp.generation
    );
    if let Some(prev) = *prev_generation {
        assert!(
            generation != prev,
            "{lane} {phase}: expected fresh actor generation, prev={prev} current={generation}"
        );
    }
    *prev_generation = Some(generation);
    actor.shutdown();
}

fn run_af_ephemeral_compute_sample(cfg: &Config, producers: usize) -> Duration {
    let ops = cfg.ops;
    let warmup = cfg.warmup;
    let cap = cfg.cap;
    run_workers(producers, move |worker_idx, ready, start| {
        let processed = Arc::new(AtomicU64::new(0));
        let seen_generation = Arc::new(AtomicU64::new(0));
        let next_generation = Arc::new(AtomicU64::new(1));
        let processed_for_make = Arc::clone(&processed);
        let seen_generation_for_make = Arc::clone(&seen_generation);
        let next_generation_for_make = Arc::clone(&next_generation);

        let pool = sync::default_pool::<AfEphemeralComputeActor>();
        let (addr, handle) = pool.spawn_reusable(cap, move || {
            let generation = next_generation_for_make.fetch_add(1, Ordering::Relaxed);
            AfEphemeralComputeActor {
                processed: Arc::clone(&processed_for_make),
                seen_generation: Arc::clone(&seen_generation_for_make),
                generation,
            }
        });
        assert!(
            handle.wait_for_startup(),
            "af_ephemeral_compute worker={worker_idx}: failed before startup"
        );

        let mut token = (worker_idx as u64).wrapping_mul(1_000_000_000);
        let mut prev_generation = None;
        for _ in 0..warmup {
            token = token.wrapping_add(1);
            af_compute_round(
                &addr,
                &handle,
                processed.as_ref(),
                seen_generation.as_ref(),
                token,
                &mut prev_generation,
                "warmup",
            );
        }

        ready.send(()).expect("failed to signal warmup ready");
        wait_for_start_signal(start.as_ref());

        for _ in 0..ops {
            token = token.wrapping_add(1);
            af_compute_round(
                &addr,
                &handle,
                processed.as_ref(),
                seen_generation.as_ref(),
                token,
                &mut prev_generation,
                "measure",
            );
        }

        handle.shutdown();
    })
}

fn run_af_ephemeral_ask_sample(cfg: &Config, producers: usize) -> Duration {
    let ops = cfg.ops;
    let warmup = cfg.warmup;
    let cap = cfg.cap;
    run_workers(producers, move |worker_idx, ready, start| {
        let next_generation = Arc::new(AtomicU64::new(1));
        let next_generation_for_make = Arc::clone(&next_generation);
        let pool = sync::default_pool::<AfEphemeralAskActor>();
        let (addr, handle) = pool.spawn_reusable(cap, move || {
            let generation = next_generation_for_make.fetch_add(1, Ordering::Relaxed);
            AfEphemeralAskActor { generation }
        });
        assert!(
            handle.wait_for_startup(),
            "af_ephemeral_ask worker={worker_idx}: failed before startup"
        );

        let mut token = (worker_idx as u64).wrapping_mul(1_000_000_000);
        let mut prev_generation = None;
        for _ in 0..warmup {
            token = token.wrapping_add(1);
            af_ask_round(&addr, &handle, token, &mut prev_generation, "warmup");
        }

        ready.send(()).expect("failed to signal warmup ready");
        wait_for_start_signal(start.as_ref());

        for _ in 0..ops {
            token = token.wrapping_add(1);
            af_ask_round(&addr, &handle, token, &mut prev_generation, "measure");
        }

        handle.shutdown();
    })
}

fn run_direct_reuse_compute_sample(cfg: &Config, producers: usize) -> Duration {
    let ops = cfg.ops;
    let warmup = cfg.warmup;
    run_workers(producers, move |worker_idx, ready, start| {
        let processed = Arc::new(AtomicU64::new(0));
        let seen_generation = Arc::new(AtomicU64::new(0));
        let processed2 = Arc::clone(&processed);
        let seen2 = Arc::clone(&seen_generation);
        let mut actor = direct::Actor::new(move || AfEphemeralComputeActor {
            processed: Arc::clone(&processed2),
            seen_generation: Arc::clone(&seen2),
            generation: 1,
        });

        let mut token = (worker_idx as u64).wrapping_mul(1_000_000_000);
        let mut pinned_generation = None;
        for _ in 0..warmup {
            token = token.wrapping_add(1);
            direct_reuse_compute_round(
                &mut actor,
                processed.as_ref(),
                seen_generation.as_ref(),
                token,
                &mut pinned_generation,
                "warmup",
            );
        }

        ready.send(()).expect("failed to signal warmup ready");
        wait_for_start_signal(start.as_ref());

        for _ in 0..ops {
            token = token.wrapping_add(1);
            direct_reuse_compute_round(
                &mut actor,
                processed.as_ref(),
                seen_generation.as_ref(),
                token,
                &mut pinned_generation,
                "measure",
            );
        }
        actor.shutdown();
    })
}

fn run_direct_reuse_ask_sample(cfg: &Config, producers: usize) -> Duration {
    let ops = cfg.ops;
    let warmup = cfg.warmup;
    run_workers(producers, move |worker_idx, ready, start| {
        let mut actor = direct::Actor::new(|| DirectAskActor { generation: 1 });

        let mut token = (worker_idx as u64).wrapping_mul(1_000_000_000);
        let mut pinned_generation = None;
        for _ in 0..warmup {
            token = token.wrapping_add(1);
            direct_reuse_ask_round(&mut actor, token, &mut pinned_generation, "warmup");
        }

        ready.send(()).expect("failed to signal warmup ready");
        wait_for_start_signal(start.as_ref());

        for _ in 0..ops {
            token = token.wrapping_add(1);
            direct_reuse_ask_round(&mut actor, token, &mut pinned_generation, "measure");
        }
        actor.shutdown();
    })
}

fn run_direct_respawn_compute_sample(cfg: &Config, producers: usize) -> Duration {
    let ops = cfg.ops;
    let warmup = cfg.warmup;
    run_workers(producers, move |worker_idx, ready, start| {
        let processed = Arc::new(AtomicU64::new(0));
        let seen_generation = Arc::new(AtomicU64::new(0));
        let mut next_generation = 1u64;
        let mut prev_generation = None;
        let mut token = (worker_idx as u64).wrapping_mul(1_000_000_000);

        for _ in 0..warmup {
            token = token.wrapping_add(1);
            direct_respawn_compute_round(
                &processed,
                &seen_generation,
                token,
                &mut next_generation,
                &mut prev_generation,
                "warmup",
            );
        }

        ready.send(()).expect("failed to signal warmup ready");
        wait_for_start_signal(start.as_ref());

        for _ in 0..ops {
            token = token.wrapping_add(1);
            direct_respawn_compute_round(
                &processed,
                &seen_generation,
                token,
                &mut next_generation,
                &mut prev_generation,
                "measure",
            );
        }
    })
}

fn run_direct_respawn_ask_sample(cfg: &Config, producers: usize) -> Duration {
    let ops = cfg.ops;
    let warmup = cfg.warmup;
    run_workers(producers, move |worker_idx, ready, start| {
        let mut next_generation = 1u64;
        let mut prev_generation = None;
        let mut token = (worker_idx as u64).wrapping_mul(1_000_000_000);

        for _ in 0..warmup {
            token = token.wrapping_add(1);
            direct_respawn_ask_round(token, &mut next_generation, &mut prev_generation, "warmup");
        }

        ready.send(()).expect("failed to signal warmup ready");
        wait_for_start_signal(start.as_ref());

        for _ in 0..ops {
            token = token.wrapping_add(1);
            direct_respawn_ask_round(token, &mut next_generation, &mut prev_generation, "measure");
        }
    })
}

fn run_lane_sample(lane: Lane, scenario: Scenario, cfg: &Config) -> Duration {
    match lane {
        Lane::AfEphemeralCompute => run_af_ephemeral_compute_sample(cfg, scenario.producers),
        Lane::AfEphemeralAsk => run_af_ephemeral_ask_sample(cfg, scenario.producers),
        Lane::DirectReuseCompute => run_direct_reuse_compute_sample(cfg, scenario.producers),
        Lane::DirectReuseAsk => run_direct_reuse_ask_sample(cfg, scenario.producers),
        Lane::DirectRespawnCompute => run_direct_respawn_compute_sample(cfg, scenario.producers),
        Lane::DirectRespawnAsk => run_direct_respawn_ask_sample(cfg, scenario.producers),
    }
}

fn measure_lane(lane: Lane, scenario: Scenario, cfg: &Config) -> LaneStats {
    let mut durations = Vec::with_capacity(cfg.samples);
    for _ in 0..cfg.samples {
        durations.push(run_lane_sample(lane, scenario, cfg));
    }
    let total_ops = cfg.ops.saturating_mul(scenario.producers as u64).max(1);
    let median_duration = median_duration(&durations);
    let median_secs = median_duration.as_secs_f64();
    let median_ops_per_sec = if median_secs > 0.0 {
        total_ops as f64 / median_secs
    } else {
        0.0
    };
    let median_ns_per_op = median_duration.as_nanos() as f64 / total_ops as f64;
    LaneStats {
        lane,
        median_ops_per_sec,
        median_ns_per_op,
        total_ops,
    }
}

fn find_ratio(
    results: &[ScenarioResult],
    workflow: Workflow,
    producers: usize,
    lane: Lane,
) -> Option<f64> {
    for r in results {
        if r.scenario.workflow == workflow && r.scenario.producers == producers {
            for (l, ratio) in &r.ratios {
                if *l == lane {
                    return Some(*ratio);
                }
            }
        }
    }
    None
}

fn main() {
    let cfg = match parse_config() {
        Ok(cfg) => cfg,
        Err(msg) => {
            eprintln!("{msg}");
            return;
        }
    };

    let max_producers = cfg.producers.iter().copied().max().unwrap_or(1);
    match sync::set_default_pool_config(sync::PoolConfig {
        size: max_producers,
        spin_init: 64,
    }) {
        Ok(()) => {}
        Err(existing) => {
            if existing.size != max_producers || existing.spin_init != 64 {
                panic!(
                    "sync default pool already initialized with size={} spin_init={}, requested size={} spin_init={}",
                    existing.size, existing.spin_init, max_producers, 64
                );
            }
        }
    }

    println!("== ephemeral_slacktor_poc ==");
    println!(
        "config: ops_per_producer={} warmup_per_producer={} samples={} cap={} producers={:?}",
        cfg.ops, cfg.warmup, cfg.samples, cfg.cap, cfg.producers
    );
    println!(
        "go_policy: direct_respawn_* must be >= x{:.3} of AF baseline for compute and ask at producers=1",
        GO_THRESHOLD_RATIO
    );
    println!();

    let mut results = Vec::new();

    for &producers in &cfg.producers {
        for workflow in [Workflow::Compute, Workflow::Ask] {
            let scenario = Scenario {
                workflow,
                producers,
            };
            let mut lane_stats = Vec::new();
            for &lane in lanes_for_workflow(workflow) {
                lane_stats.push(measure_lane(lane, scenario, &cfg));
            }

            let baseline_lane = baseline_lane_for_workflow(workflow);
            let baseline_ops_per_sec = lane_stats
                .iter()
                .find(|s| s.lane == baseline_lane)
                .map(|s| s.median_ops_per_sec)
                .unwrap_or(0.0);
            let ratios: Vec<(Lane, f64)> = lane_stats
                .iter()
                .map(|s| {
                    let r = if baseline_ops_per_sec > 0.0 {
                        s.median_ops_per_sec / baseline_ops_per_sec
                    } else {
                        0.0
                    };
                    (s.lane, r)
                })
                .collect();

            println!("== scenario: {} ==", scenario.label());
            println!(
                "{:<28} {:<24} {:>14} {:>14} {:>14}",
                "lane", "mode", "median ops/s", "median ns/op", "total ops"
            );
            for s in &lane_stats {
                println!(
                    "{:<28} {:<24} {:>14.3} {:>14.3} {:>14}",
                    s.lane.as_str(),
                    s.lane.mode(),
                    s.median_ops_per_sec,
                    s.median_ns_per_op,
                    s.total_ops
                );
            }
            println!("ratio_vs_af_baseline (baseline=1.000):");
            for (lane, ratio) in &ratios {
                println!("  {:<28} x{:.3}", lane.as_str(), ratio);
            }
            println!();

            results.push(ScenarioResult {
                scenario,
                lanes: lane_stats,
                ratios,
            });
        }
    }

    let respawn_compute_ratio =
        find_ratio(&results, Workflow::Compute, 1, Lane::DirectRespawnCompute);
    let respawn_ask_ratio = find_ratio(&results, Workflow::Ask, 1, Lane::DirectRespawnAsk);
    let reuse_compute_ratio = find_ratio(&results, Workflow::Compute, 1, Lane::DirectReuseCompute);
    let reuse_ask_ratio = find_ratio(&results, Workflow::Ask, 1, Lane::DirectReuseAsk);

    let go = matches!(respawn_compute_ratio, Some(r) if r >= GO_THRESHOLD_RATIO)
        && matches!(respawn_ask_ratio, Some(r) if r >= GO_THRESHOLD_RATIO);

    println!("== verdict ==");
    if go {
        println!("VERDICT: GO");
        println!(
            "reason: respawn parity lanes passed threshold at producers=1: compute=x{:.3}, ask=x{:.3}",
            respawn_compute_ratio.unwrap_or(0.0),
            respawn_ask_ratio.unwrap_or(0.0)
        );
    } else {
        println!("VERDICT: NO_GO");
        match (respawn_compute_ratio, respawn_ask_ratio) {
            (Some(c), Some(a)) => {
                println!(
                    "threshold miss: need both >= x{:.3}; got compute=x{:.3}, ask=x{:.3}",
                    GO_THRESHOLD_RATIO, c, a
                );
            }
            _ => {
                println!(
                    "threshold evaluation unavailable: missing producers=1 parity scenario (required for GO decision)"
                );
            }
        }

        let reuse_compute = reuse_compute_ratio.unwrap_or(0.0);
        let reuse_ask = reuse_ask_ratio.unwrap_or(0.0);
        if reuse_compute >= GO_THRESHOLD_RATIO || reuse_ask >= GO_THRESHOLD_RATIO {
            println!(
                "note: reuse lane win is upper-bound only (compute=x{:.3}, ask=x{:.3}); replacement verdict remains NO_GO",
                reuse_compute, reuse_ask
            );
        }
    }
    println!();

    println!(
        "SUMMARY respawn_compute_ratio={} respawn_ask_ratio={} reuse_compute_ratio={} reuse_ask_ratio={}",
        respawn_compute_ratio.unwrap_or(-1.0),
        respawn_ask_ratio.unwrap_or(-1.0),
        reuse_compute_ratio.unwrap_or(-1.0),
        reuse_ask_ratio.unwrap_or(-1.0)
    );

    for scenario in &results {
        for lane in &scenario.lanes {
            black_box(lane.median_ops_per_sec);
            black_box(lane.median_ns_per_op);
        }
    }
}
