use std::future::Future;
use std::io::{BufRead, BufReader, Write};
use std::net::SocketAddr;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Once;
use std::time::{Duration, Instant};

use futures::StreamExt;

use bytes::Bytes;
use icanact_core::remote::{self as af_remote, NetworkRef, RemoteMailboxAddr, RemoteTransportHandle, try_tell_network};
use icanact_remote::registry::{ActorMessageHandlerSync, ActorResponse};
use icanact_remote::{AlignedBytes, GossipConfig, GossipRegistryHandle, KeyPair, WireType};
use kameo::actor::{ActorRef as KameoActorRef, RemoteActorRef as KameoRemoteActorRef, Spawn as KameoSpawn};
use kameo::message::{Context as KameoContext, Message as KameoMessage};
use libp2p::swarm::{NetworkBehaviour, SwarmEvent};
use libp2p::{Multiaddr, SwarmBuilder, mdns, noise, tcp, yamux};

const AF_SERVICE: &str = "bench/af/remote_compare/v1";
const KAMEO_SERVICE: &str = "bench/kameo/remote_compare/v1";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Framework {
    Af,
    Kameo,
    Both,
}

impl Framework {
    fn parse(raw: &str) -> Result<Self, String> {
        match raw {
            "af" => Ok(Self::Af),
            "kameo" => Ok(Self::Kameo),
            "both" => Ok(Self::Both),
            other => Err(format!("invalid --framework '{other}', expected af|kameo|both")),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Workflow {
    Tell,
    TellConfirm,
    Ask,
}

impl Workflow {
    fn parse(raw: &str) -> Result<Self, String> {
        match raw {
            "tell" => Ok(Self::Tell),
            "tell-confirm" => Ok(Self::TellConfirm),
            "ask" => Ok(Self::Ask),
            other => Err(format!("invalid --workflow '{other}', expected tell|tell-confirm|ask")),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Role {
    Runner,
    ServerAf,
    ServerKameo,
}

#[derive(Clone, Debug)]
struct Config {
    role: Role,
    framework: Framework,
    workflow: Workflow,
    ops: u64,
    warmup: u64,
    threads: usize,
    inflight: usize,
    af_bind: SocketAddr,
    kameo_server_addr: &'static str,
    kameo_client_addr: &'static str,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            role: Role::Runner,
            framework: Framework::Both,
            workflow: Workflow::Tell,
            ops: 100_000,
            warmup: 5_000,
            threads: 1,
            inflight: 1,
            af_bind: "127.0.0.1:0".parse().unwrap(),
            kameo_server_addr: "/ip4/127.0.0.1/tcp/41010",
            kameo_client_addr: "/ip4/127.0.0.1/tcp/41011",
        }
    }
}

fn usage() -> &'static str {
    "Usage: cargo bench --features remote --bench remote_framework_compare -- [options]\n\
     \n\
     Runner options:\n\
       --framework af|kameo|both   Which frameworks to benchmark (default: both)\n\
       --workflow tell|tell-confirm|ask   Which operation to measure (default: tell)\n\
       --ops N                     Timed operations (default: 100000)\n\
       --warmup N                  Warmup operations (default: 5000)\n\
       --threads N                 Tokio worker threads (default: 1)\n\
       --inflight N                Max in-flight asks for ask workflow (default: 1)\n\
     \n\
     Internal server roles:\n\
       --role server-af|server-kameo\n"
}

fn parse_config() -> Result<Config, String> {
    let mut cfg = Config::default();
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--bench" => {}
            "--role" => {
                cfg.role = match it.next().ok_or_else(|| "missing value for --role".to_string())?.as_str() {
                    "server-af" => Role::ServerAf,
                    "server-kameo" => Role::ServerKameo,
                    other => return Err(format!("invalid role: {other}")),
                };
            }
            "--framework" => {
                cfg.framework = Framework::parse(&it.next().ok_or_else(|| "missing value for --framework".to_string())?)?;
            }
            "--workflow" => {
                cfg.workflow = Workflow::parse(&it.next().ok_or_else(|| "missing value for --workflow".to_string())?)?;
            }
            "--ops" => {
                cfg.ops = it.next().ok_or_else(|| "missing value for --ops".to_string())?
                    .parse::<u64>().map_err(|_| "invalid integer for --ops".to_string())?.max(1);
            }
            "--warmup" => {
                cfg.warmup = it.next().ok_or_else(|| "missing value for --warmup".to_string())?
                    .parse::<u64>().map_err(|_| "invalid integer for --warmup".to_string())?;
            }
            "--threads" => {
                cfg.threads = it.next().ok_or_else(|| "missing value for --threads".to_string())?
                    .parse::<usize>().map_err(|_| "invalid integer for --threads".to_string())?.max(1);
            }
            "--inflight" => {
                cfg.inflight = it.next().ok_or_else(|| "missing value for --inflight".to_string())?
                    .parse::<usize>().map_err(|_| "invalid integer for --inflight".to_string())?.max(1);
            }
            "--help" | "-h" => return Err(usage().to_string()),
            other => return Err(format!("unknown arg: {other}\n\n{}", usage())),
        }
    }
    Ok(cfg)
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Copy, Debug)]
struct AfTellMsg {
    v: u64,
}
icanact_remote::wire_type!(AfTellMsg, "icanact-examples.remote_compare.AfTellMsg/v1");

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Copy, Debug)]
struct AfAskMsg {
    v: u64,
}
icanact_remote::wire_type!(AfAskMsg, "icanact-examples.remote_compare.AfAskMsg/v1");

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Copy, Debug)]
struct AfBarrierMsg {
    phase: u8,
}
icanact_remote::wire_type!(AfBarrierMsg, "icanact-examples.remote_compare.AfBarrierMsg/v1");

#[derive(Clone)]
struct AfServerHandler {
    delivered: Arc<AtomicU64>,
}

impl ActorMessageHandlerSync for AfServerHandler {
    fn handle_actor_message_sync(
        &self,
        actor_id: u64,
        type_hash: u32,
        payload: AlignedBytes,
        _correlation_id: Option<u16>,
    ) -> icanact_remote::Result<Option<ActorResponse>> {
        if actor_id != af_remote::actor_id_of(AF_SERVICE) {
            return Ok(None);
        }

        let tell_hash = af_remote::wire_hash32(AfTellMsg::TYPE_HASH);
        let ask_hash = af_remote::wire_hash32(AfAskMsg::TYPE_HASH);
        let barrier_hash = af_remote::wire_hash32(AfBarrierMsg::TYPE_HASH);
        if type_hash == tell_hash {
            self.delivered.fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        }
        if type_hash == ask_hash {
            self.delivered.fetch_add(1, Ordering::Relaxed);
            return Ok(Some(ActorResponse::Aligned(payload)));
        }
        if type_hash == barrier_hash {
            let msg: AfBarrierMsg = icanact_remote::decode_typed(payload.as_ref())?;
            print_barrier("af", msg.phase, self.delivered.load(Ordering::Acquire));
            return Ok(None);
        }
        Ok(None)
    }
}

#[derive(kameo::Actor, kameo::RemoteActor)]
struct KameoBenchActor {
    delivered: Arc<AtomicU64>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct KameoTell {
    v: u64,
}

#[kameo::remote_message("remote_compare_tell")]
impl KameoMessage<KameoTell> for KameoBenchActor {
    type Reply = ();
    async fn handle(&mut self, _msg: KameoTell, _ctx: &mut KameoContext<Self, Self::Reply>) -> Self::Reply {
        self.delivered.fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct KameoAsk {
    v: u64,
}

#[kameo::remote_message("remote_compare_ask")]
impl KameoMessage<KameoAsk> for KameoBenchActor {
    type Reply = u64;
    async fn handle(&mut self, msg: KameoAsk, _ctx: &mut KameoContext<Self, Self::Reply>) -> Self::Reply {
        self.delivered.fetch_add(1, Ordering::Relaxed);
        msg.v
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct KameoBarrier {
    phase: u8,
}

#[kameo::remote_message("remote_compare_barrier")]
impl KameoMessage<KameoBarrier> for KameoBenchActor {
    type Reply = ();
    async fn handle(&mut self, msg: KameoBarrier, _ctx: &mut KameoContext<Self, Self::Reply>) -> Self::Reply {
        print_barrier("kameo", msg.phase, self.delivered.load(Ordering::Acquire));
    }
}

#[derive(NetworkBehaviour)]
struct KameoBenchBehaviour {
    kameo: kameo::remote::Behaviour,
    mdns: mdns::tokio::Behaviour,
}

fn run_runtime<T>(threads: usize, fut: impl Future<Output = T>) -> T {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(fut)
}

fn print_ready(line: &str) {
    println!("{line}");
    std::io::stdout().flush().expect("flush stdout");
}

fn print_barrier(framework: &str, phase: u8, count: u64) {
    println!("BARRIER framework={framework} phase={phase} count={count}");
    std::io::stdout().flush().expect("flush stdout");
}

fn payload_at(i: u64) -> u64 {
    let mut x = i.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x ^= x >> 30;
    x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

fn assert_optimized_bench_binary() {
    let exe = std::env::current_exe().expect("bench exe path");
    assert!(
        !cfg!(debug_assertions),
        "remote benchmarks must run as optimized bench binaries; rerun with `cargo bench ...` (current exe: {})",
        exe.display()
    );
}

fn ensure_tls_provider() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        rustls::crypto::ring::default_provider()
            .install_default()
            .ok();
    });
}

fn server_af(cfg: &Config) -> icanact_remote::Result<()> {
    run_runtime(cfg.threads, async move {
        ensure_tls_provider();
        let config = GossipConfig {
            gossip_interval: Duration::from_secs(5),
            ask_window: 65_536,
            ..Default::default()
        };
        let keypair = KeyPair::new_for_testing("examples-remote-af-server");
        let handle = GossipRegistryHandle::new_with_transport_stack(
            cfg.af_bind,
            keypair.to_secret_key(),
            Some(config),
            icanact_remote::BuilderTlsBootstrap,
        )
        .await?;
        let delivered = Arc::new(AtomicU64::new(0));
        handle
            .registry
            .set_actor_message_handler_sync(Arc::new(AfServerHandler {
                delivered: Arc::clone(&delivered),
            }))
            .await;
        print_ready(&format!(
            "READY framework=af addr={} peer={}",
            handle.registry.bind_addr,
            handle.registry.peer_id.to_hex()
        ));
        futures::future::pending::<()>().await;
        #[allow(unreachable_code)]
        Ok::<(), icanact_remote::GossipError>(())
    })?;
    Ok(())
}

fn client_af(
    cfg: &Config,
    server_addr: SocketAddr,
    server_peer: &str,
    barrier_rx: Option<&Receiver<String>>,
) -> icanact_remote::Result<f64> {
    run_runtime(cfg.threads, async move {
        ensure_tls_provider();
        let config = GossipConfig {
            gossip_interval: Duration::from_secs(5),
            ask_window: 65_536,
            ..Default::default()
        };
        let keypair = KeyPair::new_for_testing("examples-remote-af-client");
        let handle = GossipRegistryHandle::new_with_transport_stack(
            "127.0.0.1:0".parse().unwrap(),
            keypair.to_secret_key(),
            Some(config),
            icanact_remote::BuilderTlsBootstrap,
        )
        .await?;
        let peer_id = icanact_remote::PeerId::from_hex(server_peer).expect("peer id");
        let peer = handle.add_peer(&peer_id).await;
        peer.connect(&server_addr).await?;
        let actor_id = af_remote::actor_id_of(AF_SERVICE);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let pid = loop {
            let peer_ref = handle.lookup_peer(&peer_id).await?;
            if let Some(conn) = peer_ref.connection_ref() {
                break RemoteTransportHandle::typed::<AfTellMsg>(conn, actor_id);
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(icanact_remote::GossipError::ActorNotFound(
                    "af direct peer connection timeout".to_string(),
                ));
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        };
        let tell_ref = NetworkRef::new(RemoteMailboxAddr::Remote(pid.clone()));
        let warmup_tells: Vec<AfTellMsg> =
            (0..cfg.warmup).map(|i| AfTellMsg { v: payload_at(i) }).collect();
        let run_tells: Vec<AfTellMsg> =
            (cfg.warmup..cfg.warmup + cfg.ops)
                .map(|i| AfTellMsg { v: payload_at(i) })
                .collect();
        match cfg.workflow {
            Workflow::Tell => {
                for msg in &warmup_tells {
                    loop {
                        match try_tell_network(&tell_ref, *msg) {
                            Ok(()) => break,
                            Err(af_remote::TryTellError::Full(_)) => std::hint::spin_loop(),
                            Err(af_remote::TryTellError::Transport { err, .. }) => return Err(err),
                        }
                    }
                }
                let start = Instant::now();
                for msg in &run_tells {
                    loop {
                        match try_tell_network(&tell_ref, *msg) {
                            Ok(()) => break,
                            Err(af_remote::TryTellError::Full(_)) => std::hint::spin_loop(),
                            Err(af_remote::TryTellError::Transport { err, .. }) => return Err(err),
                        }
                    }
                }
                Ok(cfg.ops as f64 / start.elapsed().as_secs_f64())
            }
            Workflow::TellConfirm => {
                for msg in &warmup_tells {
                    loop {
                        match try_tell_network(&tell_ref, *msg) {
                            Ok(()) => break,
                            Err(af_remote::TryTellError::Full(_)) => std::hint::spin_loop(),
                            Err(af_remote::TryTellError::Transport { err, .. }) => return Err(err),
                        }
                    }
                }
                let barrier_ref = NetworkRef::new(RemoteMailboxAddr::Remote(
                    RemoteTransportHandle::typed::<AfBarrierMsg>(pid.connection().clone(), actor_id),
                ));
                try_tell_network(&barrier_ref, AfBarrierMsg { phase: 0 })
                    .map_err(|e| icanact_remote::GossipError::InvalidConfig(format!("{e:?}")))?;
                wait_barrier(
                    barrier_rx.expect("af tell-confirm requires barrier receiver"),
                    "af",
                    0,
                    cfg.warmup,
                    Duration::from_secs(15),
                )
                .map_err(|e| icanact_remote::GossipError::InvalidConfig(e.to_string()))?;
                let start = Instant::now();
                for msg in &run_tells {
                    loop {
                        match try_tell_network(&tell_ref, *msg) {
                            Ok(()) => break,
                            Err(af_remote::TryTellError::Full(_)) => std::hint::spin_loop(),
                            Err(af_remote::TryTellError::Transport { err, .. }) => return Err(err),
                        }
                    }
                }
                try_tell_network(&barrier_ref, AfBarrierMsg { phase: 1 })
                    .map_err(|e| icanact_remote::GossipError::InvalidConfig(format!("{e:?}")))?;
                wait_barrier(
                    barrier_rx.expect("af tell-confirm requires barrier receiver"),
                    "af",
                    1,
                    cfg.warmup + cfg.ops,
                    Duration::from_secs(30),
                )
                .map_err(|e| icanact_remote::GossipError::InvalidConfig(e.to_string()))?;
                Ok(cfg.ops as f64 / start.elapsed().as_secs_f64())
            }
            Workflow::Ask => {
                let conn = pid.connection().clone();
                let req = icanact_remote::encode_typed(&AfAskMsg { v: 7 })?;
                let run_asks = |count: u64| {
                    let conn = conn.clone();
                    let req = req.clone();
                    async move {
                        let mut pending: futures::stream::FuturesUnordered<
                            std::pin::Pin<Box<dyn Future<Output = icanact_remote::Result<Bytes>> + Send>>,
                        > = futures::stream::FuturesUnordered::new();
                        let mut issued = 0u64;
                        while issued < count && pending.len() < cfg.inflight {
                            let conn = conn.clone();
                            let req = req.clone();
                            pending.push(Box::pin(async move {
                                let resp = conn.ask_direct_no_timeout(req).await?;
                                Ok(resp)
                            }));
                            issued += 1;
                        }
                        while let Some(result) = pending.next().await {
                            let resp = result?;
                            assert_eq!(resp.as_ref(), req.as_ref());
                            if issued < count {
                                let conn = conn.clone();
                                let req = req.clone();
                                pending.push(Box::pin(async move {
                                    let resp = conn.ask_direct_no_timeout(req).await?;
                                    Ok(resp)
                                }));
                                issued += 1;
                            }
                        }
                        Ok::<(), icanact_remote::GossipError>(())
                    }
                };
                run_asks(cfg.warmup).await?;
                let start = Instant::now();
                run_asks(cfg.ops).await?;
                Ok(cfg.ops as f64 / start.elapsed().as_secs_f64())
            }
        }
    })
}

fn spawn_kameo_swarm(listen_addr: &str, dial_addr: Option<&str>, emit_ready: bool) -> Result<(), Box<dyn std::error::Error>> {
    let mut swarm = SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(tcp::Config::default(), noise::Config::new, yamux::Config::default)?
        .with_quic()
        .with_behaviour(|key| {
            let local_peer_id = key.public().to_peer_id();
            let kameo = kameo::remote::Behaviour::new(local_peer_id, kameo::remote::messaging::Config::default());
            let mdns = mdns::tokio::Behaviour::new(mdns::Config::default(), local_peer_id)?;
            Ok(KameoBenchBehaviour { kameo, mdns })
        })?
        .build();

    swarm.behaviour().kameo.try_init_global()?;
    swarm.listen_on(listen_addr.parse::<Multiaddr>()?)?;
    if let Some(addr) = dial_addr {
        swarm.dial(addr.parse::<Multiaddr>()?)?;
    }

    tokio::spawn(async move {
        loop {
            match swarm.select_next_some().await {
                SwarmEvent::Behaviour(KameoBenchBehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
                    for (peer_id, multiaddr) in list {
                        swarm.add_peer_address(peer_id, multiaddr);
                    }
                }
                SwarmEvent::Behaviour(KameoBenchBehaviourEvent::Mdns(mdns::Event::Expired(list))) => {
                    for (peer_id, _multiaddr) in list {
                        let _ = swarm.disconnect_peer_id(peer_id);
                    }
                }
                SwarmEvent::NewListenAddr { address, .. } => {
                    if emit_ready {
                        print_ready(&format!("READY framework=kameo addr={address}"));
                    }
                }
                _ => {}
            }
        }
    });
    Ok(())
}

fn server_kameo(cfg: &Config) -> Result<(), Box<dyn std::error::Error>> {
    run_runtime(cfg.threads, async move {
        spawn_kameo_swarm(cfg.kameo_server_addr, None, true)?;
        let actor_ref: KameoActorRef<KameoBenchActor> = KameoBenchActor::spawn(KameoBenchActor {
            delivered: Arc::new(AtomicU64::new(0)),
        });
        actor_ref.register(KAMEO_SERVICE).await?;
        futures::future::pending::<()>().await;
        #[allow(unreachable_code)]
        Ok::<(), Box<dyn std::error::Error>>(())
    })
}

fn client_kameo(
    cfg: &Config,
    server_addr: &str,
    barrier_rx: Option<&Receiver<String>>,
) -> Result<f64, Box<dyn std::error::Error>> {
    run_runtime(cfg.threads, async move {
        spawn_kameo_swarm(cfg.kameo_client_addr, Some(server_addr), false)?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        let remote_ref = loop {
            if let Some(actor) = KameoRemoteActorRef::<KameoBenchActor>::lookup(KAMEO_SERVICE).await? {
                break actor;
            }
            if tokio::time::Instant::now() >= deadline {
                return Err::<f64, Box<dyn std::error::Error>>("kameo lookup timeout".into());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        async fn kameo_echo_retry(
            remote_ref: &KameoRemoteActorRef<KameoBenchActor>,
            v: u64,
        ) -> Result<u64, Box<dyn std::error::Error>> {
            loop {
                match remote_ref.ask(&KameoAsk { v }).send().await {
                    Ok(out) => return Ok(out),
                    Err(err) if err.to_string().contains("max sub-streams reached") => std::thread::yield_now(),
                    Err(err) => return Err(err.into()),
                }
            }
        }

        fn kameo_tell_retry(
            remote_ref: &KameoRemoteActorRef<KameoBenchActor>,
            msg: &KameoTell,
        ) -> Result<(), Box<dyn std::error::Error>> {
            loop {
                match remote_ref.tell(msg).send() {
                    Ok(()) => return Ok(()),
                    Err(err) if err.to_string().contains("max sub-streams reached") => std::thread::yield_now(),
                    Err(err) => return Err(err.into()),
                }
            }
        }

        fn kameo_barrier_retry(
            remote_ref: &KameoRemoteActorRef<KameoBenchActor>,
            msg: &KameoBarrier,
        ) -> Result<(), Box<dyn std::error::Error>> {
            loop {
                match remote_ref.tell(msg).send() {
                    Ok(()) => return Ok(()),
                    Err(err) if err.to_string().contains("max sub-streams reached") => {
                        std::thread::yield_now()
                    }
                    Err(err) => return Err(err.into()),
                }
            }
        }
        let warmup_tells: Vec<KameoTell> =
            (0..cfg.warmup).map(|i| KameoTell { v: payload_at(i) }).collect();
        let run_tells: Vec<KameoTell> =
            (cfg.warmup..cfg.warmup + cfg.ops)
                .map(|i| KameoTell { v: payload_at(i) })
                .collect();
        match cfg.workflow {
            Workflow::Tell => {
                for msg in &warmup_tells {
                    kameo_tell_retry(&remote_ref, msg)?;
                }
                let start = Instant::now();
                for msg in &run_tells {
                    kameo_tell_retry(&remote_ref, msg)?;
                }
                Ok(cfg.ops as f64 / start.elapsed().as_secs_f64())
            }
            Workflow::TellConfirm => {
                for msg in &warmup_tells {
                    kameo_tell_retry(&remote_ref, msg)?;
                }
                kameo_barrier_retry(&remote_ref, &KameoBarrier { phase: 0 })?;
                wait_barrier(
                    barrier_rx.expect("kameo tell-confirm requires barrier receiver"),
                    "kameo",
                    0,
                    cfg.warmup,
                    Duration::from_secs(15),
                )?;
                let start = Instant::now();
                for msg in &run_tells {
                    kameo_tell_retry(&remote_ref, msg)?;
                }
                kameo_barrier_retry(&remote_ref, &KameoBarrier { phase: 1 })?;
                wait_barrier(
                    barrier_rx.expect("kameo tell-confirm requires barrier receiver"),
                    "kameo",
                    1,
                    cfg.warmup + cfg.ops,
                    Duration::from_secs(30),
                )?;
                Ok(cfg.ops as f64 / start.elapsed().as_secs_f64())
            }
            Workflow::Ask => {
                let run_asks = |count: u64| {
                    let remote_ref = remote_ref.clone();
                    async move {
                        let mut pending: futures::stream::FuturesUnordered<
                            std::pin::Pin<Box<dyn Future<Output = Result<(u64, u64), Box<dyn std::error::Error>>> + Send>>,
                        > = futures::stream::FuturesUnordered::new();
                        let mut next = 0u64;
                        while next < count && pending.len() < cfg.inflight {
                            let remote_ref = remote_ref.clone();
                            let value = next;
                            pending.push(Box::pin(async move {
                                let out = kameo_echo_retry(&remote_ref, value).await?;
                                Ok((value, out))
                            }));
                            next += 1;
                        }
                        while let Some(result) = pending.next().await {
                            let (expected, out) = result?;
                            assert_eq!(out, expected);
                            if next < count {
                                let remote_ref = remote_ref.clone();
                                let value = next;
                                pending.push(Box::pin(async move {
                                    let out = kameo_echo_retry(&remote_ref, value).await?;
                                    Ok((value, out))
                                }));
                                next += 1;
                            }
                        }
                        Ok::<(), Box<dyn std::error::Error>>(())
                    }
                };
                run_asks(cfg.warmup).await?;
                let start = Instant::now();
                run_asks(cfg.ops).await?;
                Ok(cfg.ops as f64 / start.elapsed().as_secs_f64())
            }
        }
    })
}

fn spawn_server(
    role: &str,
    workflow: Workflow,
    threads: usize,
    inflight: usize,
) -> (Child, String, Receiver<String>) {
    assert_optimized_bench_binary();
    let exe = std::env::current_exe().expect("bench exe");
    let mut child = Command::new(exe)
        .arg("--role").arg(role)
        .arg("--workflow").arg(match workflow {
            Workflow::Tell => "tell",
            Workflow::TellConfirm => "tell-confirm",
            Workflow::Ask => "ask",
        })
        .arg("--threads").arg(threads.to_string())
        .arg("--inflight").arg(inflight.to_string())
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn server child");
    let stdout = child.stdout.take().expect("child stdout");
    let (tx, rx) = mpsc::channel::<String>();
    std::thread::spawn(move || {
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line) {
                Ok(0) | Err(_) => break,
                Ok(_) => {
                    let _ = tx.send(line.trim().to_string());
                }
            }
        }
    });
    loop {
        let line = rx.recv().expect("server exited before readiness");
        if line.starts_with("READY ") {
            return (child, line, rx);
        }
    }
}

fn wait_barrier(
    rx: &Receiver<String>,
    framework: &str,
    phase: u8,
    expected_at_least: u64,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        match rx.recv_timeout(remaining.min(Duration::from_millis(200))) {
            Ok(line) => {
                if !line.starts_with("BARRIER ") {
                    continue;
                }
                let mut got_framework = None::<String>;
                let mut got_phase = None::<u8>;
                let mut got_count = None::<u64>;
                for part in line.split_whitespace() {
                    if let Some(v) = part.strip_prefix("framework=") {
                        got_framework = Some(v.to_string());
                    } else if let Some(v) = part.strip_prefix("phase=") {
                        got_phase = v.parse::<u8>().ok();
                    } else if let Some(v) = part.strip_prefix("count=") {
                        got_count = v.parse::<u64>().ok();
                    }
                }
                if got_framework.as_deref() == Some(framework) && got_phase == Some(phase) {
                    let count = got_count.unwrap_or(0);
                    if count >= expected_at_least {
                        return Ok(());
                    }
                    return Err(format!(
                        "{framework} tell-confirm barrier shortfall: expected_at_least={expected_at_least} observed={count}"
                    )
                    .into());
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => continue,
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Err(format!("{framework} server stdout disconnected before barrier").into())
            }
        }
    }
    Err(format!(
        "{framework} tell-confirm barrier timeout: phase={phase} expected_at_least={expected_at_least}"
    )
    .into())
}

fn kill_child(mut child: Child) {
    let _ = child.kill();
    let _ = child.wait();
}

fn parse_ready_value(line: &str, key: &str) -> String {
    line.split_whitespace()
        .find_map(|part| part.strip_prefix(&format!("{key}=")).map(str::to_string))
        .unwrap_or_else(|| panic!("missing {key} in READY line: {line}"))
}

fn run_runner(cfg: &Config) {
    assert_optimized_bench_binary();
    let mut rows: Vec<(String, f64)> = Vec::new();

    if matches!(cfg.framework, Framework::Af | Framework::Both) {
        let (child, ready, rx) =
            spawn_server("server-af", cfg.workflow, cfg.threads, cfg.inflight);
        let addr: SocketAddr = parse_ready_value(&ready, "addr").parse().expect("af addr");
        let peer = parse_ready_value(&ready, "peer");
        let barrier_rx = matches!(cfg.workflow, Workflow::TellConfirm).then_some(&rx);
        let tput = client_af(cfg, addr, &peer, barrier_rx).expect("af client");
        rows.push(("actor_framework_remote".to_string(), tput));
        kill_child(child);
    }

    if matches!(cfg.framework, Framework::Kameo | Framework::Both) {
        let (child, ready, rx) =
            spawn_server("server-kameo", cfg.workflow, cfg.threads, cfg.inflight);
        let addr = parse_ready_value(&ready, "addr");
        let barrier_rx = matches!(cfg.workflow, Workflow::TellConfirm).then_some(&rx);
        let tput = client_kameo(cfg, &addr, barrier_rx).expect("kameo client");
        rows.push(("kameo_remote".to_string(), tput));
        kill_child(child);
    }

    println!("== remote_framework_compare ==");
    println!(
        "workflow={:?} ops={} warmup={} threads={} inflight={}",
        cfg.workflow, cfg.ops, cfg.warmup, cfg.threads, cfg.inflight
    );
    println!(
        "binary={} optimized={}",
        std::env::current_exe().expect("bench exe").display(),
        !cfg!(debug_assertions)
    );
    for (name, tput) in rows {
        let unit = match cfg.workflow {
            Workflow::Tell => "msg/s",
            Workflow::TellConfirm => "msg/s",
            Workflow::Ask => "ask/s",
        };
        println!("{name}: {:.3} {unit}", tput);
    }
}

fn main() {
    assert_optimized_bench_binary();
    let cfg = match parse_config() {
        Ok(cfg) => cfg,
        Err(msg) => {
            eprintln!("{msg}");
            std::process::exit(2);
        }
    };

    match cfg.role {
        Role::Runner => run_runner(&cfg),
        Role::ServerAf => {
            server_af(&cfg).expect("af server");
        }
        Role::ServerKameo => {
            server_kameo(&cfg).expect("kameo server");
        }
    }
}
