use std::future::Future;
use std::io::{BufRead, BufReader, Write};
use std::process::{Child, Command, Stdio};
use std::sync::Once;
use std::time::{Duration, Instant};

use futures::StreamExt;
use kameo::actor::{
    ActorRef as KameoActorRef, RemoteActorRef as KameoRemoteActorRef, Spawn as KameoSpawn,
};
use kameo::message::{Context as KameoContext, Message as KameoMessage};
use libp2p::swarm::{NetworkBehaviour, SwarmEvent};
use libp2p::{Multiaddr, SwarmBuilder, mdns, noise, tcp, yamux};
use ractor::{
    Actor as RactorActor, ActorProcessingErr as RactorActorProcessingErr,
    ActorRef as RactorActorRef, RpcReplyPort,
};
use ractor_cluster::{NodeServer, RactorClusterMessage, client_connect};

const KAMEO_SERVICE: &str = "bench/kameo/remote_ask_compare/v1";
const RACTOR_SERVICE: &str = "bench/ractor/remote_ask_compare/v1";
const RACTOR_GROUP: &str = "bench/ractor/remote_ask_compare/group/v1";
const RACTOR_COOKIE: &str = "bench-ractor-cookie";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Framework {
    Kameo,
    Ractor,
    Both,
}

impl Framework {
    fn parse(raw: &str) -> Result<Self, String> {
        match raw {
            "kameo" => Ok(Self::Kameo),
            "ractor" => Ok(Self::Ractor),
            "both" => Ok(Self::Both),
            other => Err(format!(
                "invalid --framework '{other}', expected kameo|ractor|both"
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Role {
    Runner,
    ServerKameo,
    ServerRactor,
}

#[derive(Clone, Debug)]
struct Config {
    role: Role,
    framework: Framework,
    ops: u64,
    warmup: u64,
    threads: usize,
    kameo_server_addr: &'static str,
    kameo_client_addr: &'static str,
    ractor_server_port: u16,
    ractor_client_port: u16,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            role: Role::Runner,
            framework: Framework::Both,
            ops: 1_000_000,
            warmup: 10_000,
            threads: 1,
            kameo_server_addr: "/ip4/127.0.0.1/tcp/41010",
            kameo_client_addr: "/ip4/127.0.0.1/tcp/41011",
            ractor_server_port: 41020,
            ractor_client_port: 41021,
        }
    }
}

fn usage() -> &'static str {
    "Usage: cargo bench --bench kameo_ractor_ask_compare -- [options]\n\
     \n\
     Runner options:\n\
       --framework kameo|ractor|both   Which frameworks to benchmark (default: both)\n\
       --ops N                         Timed asks (default: 1000000)\n\
       --warmup N                      Warmup asks (default: 10000)\n\
       --threads N                     Tokio worker threads (default: 1)\n\
     \n\
     Internal server roles:\n\
       --role server-kameo|server-ractor\n\
       --ractor-server-port N\n\
       --ractor-client-port N\n"
}

fn parse_config() -> Result<Config, String> {
    let mut cfg = Config::default();
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--bench" => {}
            "--role" => {
                cfg.role = match it
                    .next()
                    .ok_or_else(|| "missing value for --role".to_string())?
                    .as_str()
                {
                    "server-kameo" => Role::ServerKameo,
                    "server-ractor" => Role::ServerRactor,
                    other => return Err(format!("invalid role: {other}")),
                };
            }
            "--framework" => {
                cfg.framework = Framework::parse(
                    &it.next()
                        .ok_or_else(|| "missing value for --framework".to_string())?,
                )?;
            }
            "--ops" => {
                cfg.ops = it
                    .next()
                    .ok_or_else(|| "missing value for --ops".to_string())?
                    .parse::<u64>()
                    .map_err(|_| "invalid integer for --ops".to_string())?
                    .max(1);
            }
            "--warmup" => {
                cfg.warmup = it
                    .next()
                    .ok_or_else(|| "missing value for --warmup".to_string())?
                    .parse::<u64>()
                    .map_err(|_| "invalid integer for --warmup".to_string())?;
            }
            "--threads" => {
                cfg.threads = it
                    .next()
                    .ok_or_else(|| "missing value for --threads".to_string())?
                    .parse::<usize>()
                    .map_err(|_| "invalid integer for --threads".to_string())?
                    .max(1);
            }
            "--ractor-server-port" => {
                cfg.ractor_server_port = it
                    .next()
                    .ok_or_else(|| "missing value for --ractor-server-port".to_string())?
                    .parse::<u16>()
                    .map_err(|_| "invalid integer for --ractor-server-port".to_string())?;
            }
            "--ractor-client-port" => {
                cfg.ractor_client_port = it
                    .next()
                    .ok_or_else(|| "missing value for --ractor-client-port".to_string())?
                    .parse::<u16>()
                    .map_err(|_| "invalid integer for --ractor-client-port".to_string())?;
            }
            "--help" | "-h" => return Err(usage().to_string()),
            other => return Err(format!("unknown arg: {other}\n\n{}", usage())),
        }
    }
    Ok(cfg)
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

fn assert_optimized_bench_binary() {
    let exe = std::env::current_exe().expect("bench exe path");
    assert!(
        !cfg!(debug_assertions),
        "remote benchmarks must run as optimized bench binaries; rerun with `cargo bench ...` (current exe: {})",
        exe.display()
    );
}

fn reserve_local_port() -> u16 {
    std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .expect("bind ephemeral port")
        .local_addr()
        .expect("local addr")
        .port()
}

#[derive(kameo::Actor, kameo::RemoteActor)]
struct KameoBenchActor;

#[derive(serde::Serialize, serde::Deserialize)]
struct KameoAsk {
    v: u64,
}

#[kameo::remote_message("kameo_remote_compare_ask")]
impl KameoMessage<KameoAsk> for KameoBenchActor {
    type Reply = u64;

    async fn handle(
        &mut self,
        msg: KameoAsk,
        _ctx: &mut KameoContext<Self, Self::Reply>,
    ) -> Self::Reply {
        msg.v
    }
}

#[derive(NetworkBehaviour)]
struct KameoBenchBehaviour {
    kameo: kameo::remote::Behaviour,
    mdns: mdns::tokio::Behaviour,
}

fn spawn_kameo_swarm(
    listen_addr: &str,
    dial_addr: Option<&str>,
    emit_ready: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut swarm = SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_quic()
        .with_behaviour(|key| {
            let local_peer_id = key.public().to_peer_id();
            let kameo = kameo::remote::Behaviour::new(
                local_peer_id,
                kameo::remote::messaging::Config::default(),
            );
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
                SwarmEvent::Behaviour(KameoBenchBehaviourEvent::Mdns(mdns::Event::Discovered(
                    list,
                ))) => {
                    for (peer_id, multiaddr) in list {
                        swarm.add_peer_address(peer_id, multiaddr);
                    }
                }
                SwarmEvent::Behaviour(KameoBenchBehaviourEvent::Mdns(mdns::Event::Expired(
                    list,
                ))) => {
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
        let actor_ref: KameoActorRef<KameoBenchActor> = KameoBenchActor::spawn(KameoBenchActor);
        actor_ref.register(KAMEO_SERVICE).await?;
        futures::future::pending::<()>().await;
        #[allow(unreachable_code)]
        Ok::<(), Box<dyn std::error::Error>>(())
    })
}

fn client_kameo(cfg: &Config, server_addr: &str) -> Result<f64, Box<dyn std::error::Error>> {
    run_runtime(cfg.threads, async move {
        spawn_kameo_swarm(cfg.kameo_client_addr, Some(server_addr), false)?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        let remote_ref = loop {
            if let Some(actor) =
                KameoRemoteActorRef::<KameoBenchActor>::lookup(KAMEO_SERVICE).await?
            {
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
                    Err(err) if err.to_string().contains("max sub-streams reached") => {
                        std::thread::yield_now()
                    }
                    Err(err) => return Err(err.into()),
                }
            }
        }

        for i in 0..cfg.warmup {
            let out = kameo_echo_retry(&remote_ref, i).await?;
            assert_eq!(out, i);
        }
        let start = Instant::now();
        for i in 0..cfg.ops {
            let out = kameo_echo_retry(&remote_ref, i).await?;
            assert_eq!(out, i);
        }
        Ok(cfg.ops as f64 / start.elapsed().as_secs_f64())
    })
}

#[derive(RactorClusterMessage)]
enum RactorBenchMsg {
    #[rpc]
    Ask(u64, RpcReplyPort<u64>),
}

struct RactorBenchActor;

impl RactorActor for RactorBenchActor {
    type Msg = RactorBenchMsg;
    type State = ();
    type Arguments = ();

    async fn pre_start(
        &self,
        _myself: RactorActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, RactorActorProcessingErr> {
        Ok(())
    }

    async fn handle(
        &self,
        _myself: RactorActorRef<Self::Msg>,
        message: Self::Msg,
        _state: &mut Self::State,
    ) -> Result<(), RactorActorProcessingErr> {
        match message {
            RactorBenchMsg::Ask(v, reply) => {
                let _ = reply.send(v);
            }
        }
        Ok(())
    }
}

fn server_ractor(cfg: &Config) -> Result<(), Box<dyn std::error::Error>> {
    run_runtime(cfg.threads, async move {
        let node = NodeServer::new(
            cfg.ractor_server_port,
            RACTOR_COOKIE.to_string(),
            "bench_ractor_server".to_string(),
            "127.0.0.1".to_string(),
            None,
            None,
        )
        .with_listen_addr(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));
        let (_node_ref, _node_handle) = ractor::Actor::spawn(None, node, ())
            .await
            .expect("spawn ractor node server");
        let (actor, _handle) =
            ractor::Actor::spawn(Some(RACTOR_SERVICE.to_string()), RactorBenchActor, ())
                .await
                .expect("spawn ractor remote actor");
        ractor::pg::join(RACTOR_GROUP.to_string(), vec![actor.get_cell()]);
        tokio::time::sleep(Duration::from_millis(250)).await;
        print_ready(&format!(
            "READY framework=ractor addr=127.0.0.1:{}",
            cfg.ractor_server_port
        ));
        futures::future::pending::<()>().await;
        #[allow(unreachable_code)]
        Ok::<(), Box<dyn std::error::Error>>(())
    })
}

fn client_ractor(cfg: &Config, server_addr: &str) -> Result<f64, Box<dyn std::error::Error>> {
    run_runtime(cfg.threads, async move {
        let node = NodeServer::new(
            cfg.ractor_client_port,
            RACTOR_COOKIE.to_string(),
            "bench_ractor_client".to_string(),
            "127.0.0.1".to_string(),
            None,
            None,
        )
        .with_listen_addr(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));
        let (node_ref, _node_handle) = ractor::Actor::spawn(None, node, ())
            .await
            .expect("spawn ractor client node");

        client_connect(&node_ref, server_addr).await?;

        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        let remote_ref = loop {
            let members = ractor::pg::get_members(&RACTOR_GROUP.to_string());
            if let Some(actor) = members.first() {
                let remote_ref: ractor::ActorRef<RactorBenchMsg> = actor.clone().into();
                break remote_ref;
            }
            if tokio::time::Instant::now() >= deadline {
                return Err::<f64, Box<dyn std::error::Error>>(
                    "ractor group lookup timeout".into(),
                );
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };

        for i in 0..cfg.warmup {
            let out = ractor::call_t!(remote_ref, RactorBenchMsg::Ask, 2_000, i)
                .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })?;
            assert_eq!(out, i);
        }

        let start = Instant::now();
        for i in 0..cfg.ops {
            let out = ractor::call_t!(remote_ref, RactorBenchMsg::Ask, 2_000, i)
                .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })?;
            assert_eq!(out, i);
        }
        Ok(cfg.ops as f64 / start.elapsed().as_secs_f64())
    })
}

fn spawn_server(role: &str, cfg: &Config) -> (Child, String) {
    assert_optimized_bench_binary();
    let exe = std::env::current_exe().expect("bench exe");
    let mut child = Command::new(exe)
        .arg("--role")
        .arg(role)
        .arg("--threads")
        .arg(cfg.threads.to_string())
        .arg("--ractor-server-port")
        .arg(cfg.ractor_server_port.to_string())
        .arg("--ractor-client-port")
        .arg(cfg.ractor_client_port.to_string())
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn server child");
    let stdout = child.stdout.take().expect("child stdout");
    let mut reader = BufReader::new(stdout);
    let mut line = String::new();
    loop {
        line.clear();
        let n = reader.read_line(&mut line).expect("read child line");
        assert!(n != 0, "server exited before readiness");
        if line.starts_with("READY ") {
            return (child, line.trim().to_string());
        }
    }
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

    if matches!(cfg.framework, Framework::Kameo | Framework::Both) {
        let (child, ready) = spawn_server("server-kameo", cfg);
        let addr = parse_ready_value(&ready, "addr");
        let tput = client_kameo(cfg, &addr).expect("kameo client");
        rows.push(("kameo_remote".to_string(), tput));
        kill_child(child);
    }

    if matches!(cfg.framework, Framework::Ractor | Framework::Both) {
        let mut ractor_cfg = cfg.clone();
        ractor_cfg.ractor_server_port = reserve_local_port();
        ractor_cfg.ractor_client_port = reserve_local_port();
        let (child, ready) = spawn_server("server-ractor", &ractor_cfg);
        let addr = parse_ready_value(&ready, "addr");
        let tput = client_ractor(&ractor_cfg, &addr).expect("ractor client");
        rows.push(("ractor_remote".to_string(), tput));
        kill_child(child);
    }

    println!("== kameo_ractor_ask_compare ==");
    println!("mode=remote");
    println!("ops={}", cfg.ops);
    println!("warmup={}", cfg.warmup);
    println!("threads={}", cfg.threads);
    println!(
        "binary={} optimized={}",
        std::env::current_exe().expect("bench exe").display(),
        !cfg!(debug_assertions)
    );
    for (name, value) in rows {
        println!("{name}_ops_per_sec={value:.3}");
    }
}

fn init_tracing() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
            )
            .with_target(false)
            .without_time()
            .try_init();
    });
}

fn main() {
    init_tracing();
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
        Role::ServerKameo => {
            server_kameo(&cfg).expect("kameo server");
        }
        Role::ServerRactor => {
            server_ractor(&cfg).expect("ractor server");
        }
    }
}
