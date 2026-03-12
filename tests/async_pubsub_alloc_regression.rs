#![cfg(feature = "async-actors")]

use actix::System as ActixSystem;
use actix::prelude::Addr;
use actix::{Actor as ActixActor, Context as ActixContext, Handler, Message};
use icanact_core::local_async::AsyncActor;
use icanact_core::local_async::{self, BroadcastConfig};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

struct CountingAlloc;

static ALLOC_COUNT: AtomicU64 = AtomicU64::new(0);
static DEALLOC_COUNT: AtomicU64 = AtomicU64::new(0);
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);
static DEALLOC_BYTES: AtomicU64 = AtomicU64::new(0);

#[global_allocator]
static GLOBAL_ALLOC: CountingAlloc = CountingAlloc;

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        DEALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        DEALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[derive(Clone, Copy, Debug)]
struct AllocStats {
    alloc_count: u64,
    dealloc_count: u64,
    alloc_bytes: u64,
    dealloc_bytes: u64,
}

fn alloc_reset() {
    ALLOC_COUNT.store(0, Ordering::Relaxed);
    DEALLOC_COUNT.store(0, Ordering::Relaxed);
    ALLOC_BYTES.store(0, Ordering::Relaxed);
    DEALLOC_BYTES.store(0, Ordering::Relaxed);
}

fn alloc_snapshot() -> AllocStats {
    AllocStats {
        alloc_count: ALLOC_COUNT.load(Ordering::Relaxed),
        dealloc_count: DEALLOC_COUNT.load(Ordering::Relaxed),
        alloc_bytes: ALLOC_BYTES.load(Ordering::Relaxed),
        dealloc_bytes: DEALLOC_BYTES.load(Ordering::Relaxed),
    }
}

fn per_op(total: u64, n: u64) -> f64 {
    if n == 0 { 0.0 } else { total as f64 / n as f64 }
}

struct CountActor {
    seen: Arc<AtomicU64>,
}

impl AsyncActor for CountActor {
    type Msg = u64;

    #[inline(always)]
    fn handle_tell(&mut self, _msg: Self::Msg) -> impl std::future::Future<Output = ()> + Send {
        self.seen.fetch_add(1, Ordering::Relaxed);
        std::future::ready(())
    }
}

fn topic_idx(seq: u64, topics: usize, hot_topic: bool) -> usize {
    if hot_topic {
        0
    } else {
        (seq as usize) % topics
    }
}

fn expected_deliveries(msgs: u64, subs_per_topic: usize) -> u64 {
    msgs.saturating_mul(subs_per_topic as u64)
}

fn run_af_pubsub_alloc(hot_topic: bool, msgs: u64, warmup: u64) -> AllocStats {
    let topics = 8usize;
    let subs_per_topic = 16usize;
    let cap = 65_536usize;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_time()
        .build()
        .expect("build tokio runtime");

    rt.block_on(async move {
        let pubsub = local_async::PubSub::<u64>::with_broadcast_config(BroadcastConfig {
            stripes: 8,
            concurrent_threshold: 64,
        });
        let topic_names: Vec<String> = (0..topics).map(|i| format!("alloc/pubsub/{i}")).collect();
        let topic_handles: Vec<local_async::AsyncTopicHandle<u64>> = topic_names
            .iter()
            .map(|topic| pubsub.topic_async(topic))
            .collect();

        let delivered = Arc::new(AtomicU64::new(0));
        let mut handles = Vec::with_capacity(topics.saturating_mul(subs_per_topic));
        for topic in &topic_names {
            for _ in 0..subs_per_topic {
                let (addr, handle) = local_async::spawn_actor(
                    cap,
                    CountActor {
                        seen: Arc::clone(&delivered),
                    },
                );
                let _ = pubsub.subscribe(topic, addr);
                handles.push(handle);
            }
        }

        let warmup_target = expected_deliveries(warmup, subs_per_topic);
        for i in 0..warmup {
            let idx = topic_idx(i, topics, hot_topic);
            pubsub.publish_fast_to_async(&topic_handles[idx], 1);
        }
        while delivered.load(Ordering::Acquire) < warmup_target {
            tokio::task::yield_now().await;
        }

        delivered.store(0, Ordering::Release);
        alloc_reset();
        let target = expected_deliveries(msgs, subs_per_topic);
        for i in 0..msgs {
            let idx = topic_idx(i, topics, hot_topic);
            pubsub.publish_fast_to_async(&topic_handles[idx], 1);
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        while delivered.load(Ordering::Acquire) < target {
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for pubsub deliveries"
            );
            tokio::task::yield_now().await;
        }
        let stats = alloc_snapshot();

        for handle in handles {
            handle.shutdown().await;
        }

        stats
    })
}

#[derive(Message)]
#[rtype(result = "()")]
struct ActixTryTellMsg(u64);

struct ActixCounter {
    seen: Arc<AtomicU64>,
    mailbox_cap: usize,
}

impl ActixActor for ActixCounter {
    type Context = ActixContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.set_mailbox_capacity(self.mailbox_cap);
    }
}

impl Handler<ActixTryTellMsg> for ActixCounter {
    type Result = ();

    #[inline(always)]
    fn handle(&mut self, msg: ActixTryTellMsg, _ctx: &mut Self::Context) -> Self::Result {
        std::hint::black_box(msg.0);
        self.seen.fetch_add(1, Ordering::Relaxed);
    }
}

fn run_actix_pubsub_alloc(hot_topic: bool, msgs: u64, warmup: u64) -> AllocStats {
    let topics = 8usize;
    let subs_per_topic = 16usize;
    let cap = 65_536usize;

    let sys = ActixSystem::new();
    sys.block_on(async move {
        let delivered = Arc::new(AtomicU64::new(0));
        let mut topic_subscribers: Vec<Vec<Addr<ActixCounter>>> = Vec::with_capacity(topics);
        for _ in 0..topics {
            let mut subscribers = Vec::with_capacity(subs_per_topic);
            for _ in 0..subs_per_topic {
                subscribers.push(
                    ActixCounter {
                        seen: Arc::clone(&delivered),
                        mailbox_cap: cap,
                    }
                    .start(),
                );
            }
            topic_subscribers.push(subscribers);
        }

        let warmup_target = expected_deliveries(warmup, subs_per_topic);
        for i in 0..warmup {
            let idx = topic_idx(i, topics, hot_topic);
            for subscriber in &topic_subscribers[idx] {
                loop {
                    match subscriber.try_send(ActixTryTellMsg(1)) {
                        Ok(()) => break,
                        Err(actix::prelude::SendError::Full(_)) => tokio::task::yield_now().await,
                        Err(actix::prelude::SendError::Closed(_)) => {
                            panic!("actix subscriber closed")
                        }
                    }
                }
            }
        }
        while delivered.load(Ordering::Acquire) < warmup_target {
            tokio::task::yield_now().await;
        }

        delivered.store(0, Ordering::Release);
        alloc_reset();
        let target = expected_deliveries(msgs, subs_per_topic);
        for i in 0..msgs {
            let idx = topic_idx(i, topics, hot_topic);
            for subscriber in &topic_subscribers[idx] {
                loop {
                    match subscriber.try_send(ActixTryTellMsg(1)) {
                        Ok(()) => break,
                        Err(actix::prelude::SendError::Full(_)) => tokio::task::yield_now().await,
                        Err(actix::prelude::SendError::Closed(_)) => {
                            panic!("actix subscriber closed")
                        }
                    }
                }
            }
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        while delivered.load(Ordering::Acquire) < target {
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for actix pubsub deliveries"
            );
            tokio::task::yield_now().await;
        }

        let stats = alloc_snapshot();
        drop(topic_subscribers);
        stats
    })
}

#[test]
fn async_pubsub_cold_vs_hot_allocation_probe() {
    let msgs = 10_000u64;
    let warmup = 1_000u64;

    let cold = run_af_pubsub_alloc(false, msgs, warmup);
    let hot = run_af_pubsub_alloc(true, msgs, warmup);
    let actix_cold = run_actix_pubsub_alloc(false, msgs, warmup);
    let actix_hot = run_actix_pubsub_alloc(true, msgs, warmup);

    eprintln!(
        "alloc-cold: allocs={} ({:.3}/op), alloc_bytes={} ({:.3}/op), deallocs={} ({:.3}/op), dealloc_bytes={} ({:.3}/op)",
        cold.alloc_count,
        per_op(cold.alloc_count, msgs),
        cold.alloc_bytes,
        per_op(cold.alloc_bytes, msgs),
        cold.dealloc_count,
        per_op(cold.dealloc_count, msgs),
        cold.dealloc_bytes,
        per_op(cold.dealloc_bytes, msgs),
    );
    eprintln!(
        "alloc-hot : allocs={} ({:.3}/op), alloc_bytes={} ({:.3}/op), deallocs={} ({:.3}/op), dealloc_bytes={} ({:.3}/op)",
        hot.alloc_count,
        per_op(hot.alloc_count, msgs),
        hot.alloc_bytes,
        per_op(hot.alloc_bytes, msgs),
        hot.dealloc_count,
        per_op(hot.dealloc_count, msgs),
        hot.dealloc_bytes,
        per_op(hot.dealloc_bytes, msgs),
    );
    eprintln!(
        "alloc-delta: allocs(cold-hot)={}, alloc_bytes(cold-hot)={}, deallocs(cold-hot)={}, dealloc_bytes(cold-hot)={}",
        (cold.alloc_count as i64) - (hot.alloc_count as i64),
        (cold.alloc_bytes as i64) - (hot.alloc_bytes as i64),
        (cold.dealloc_count as i64) - (hot.dealloc_count as i64),
        (cold.dealloc_bytes as i64) - (hot.dealloc_bytes as i64),
    );
    eprintln!(
        "alloc-actix-cold: allocs={} ({:.3}/op), alloc_bytes={} ({:.3}/op), deallocs={} ({:.3}/op), dealloc_bytes={} ({:.3}/op)",
        actix_cold.alloc_count,
        per_op(actix_cold.alloc_count, msgs),
        actix_cold.alloc_bytes,
        per_op(actix_cold.alloc_bytes, msgs),
        actix_cold.dealloc_count,
        per_op(actix_cold.dealloc_count, msgs),
        actix_cold.dealloc_bytes,
        per_op(actix_cold.dealloc_bytes, msgs),
    );
    eprintln!(
        "alloc-actix-hot : allocs={} ({:.3}/op), alloc_bytes={} ({:.3}/op), deallocs={} ({:.3}/op), dealloc_bytes={} ({:.3}/op)",
        actix_hot.alloc_count,
        per_op(actix_hot.alloc_count, msgs),
        actix_hot.alloc_bytes,
        per_op(actix_hot.alloc_bytes, msgs),
        actix_hot.dealloc_count,
        per_op(actix_hot.dealloc_count, msgs),
        actix_hot.dealloc_bytes,
        per_op(actix_hot.dealloc_bytes, msgs),
    );

    assert!(
        cold.alloc_count <= hot.alloc_count.saturating_mul(4).saturating_add(1024),
        "cold allocation count exploded unexpectedly: cold={:?} hot={:?}",
        cold,
        hot
    );
    assert!(
        cold.alloc_count <= actix_cold.alloc_count,
        "expected AF cold allocs <= Actix cold allocs: af={:?} actix={:?}",
        cold,
        actix_cold
    );
}
