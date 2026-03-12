#![allow(dead_code)]

use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::env;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::Instant;

#[cfg(feature = "remote")]
use icanact_core::remote::{
    LinkDelta, LinkDeltaOp, PeerLinkGraph, RouteTable, build_weighted_route_table,
};

pub const DEFAULT_NODE_COUNTS: [u32; 2] = [100, 1000];
pub const DEFAULT_DEGREES: [u32; 3] = [10, 24, 100];

const ARTIFACT_DIR: &str = "artifacts/routing";
const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x100000001b3;

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct ScenarioMetrics {
    pub n: u32,
    pub r: u32,
    pub seed: u64,
    pub target_edges: usize,
    pub actual_edges: usize,
    pub route_entries: usize,
    pub missing_routes: usize,
    pub backup_entries: usize,
    pub primary_cost_sum: u64,
    pub graph_fingerprint: u64,
    pub route_fingerprint: u64,
    pub replay_route_fingerprint: u64,
    pub signature: u64,
    pub replay_signature: u64,
    pub replay_match: bool,
    pub build_ns: u64,
    pub replay_build_ns: u64,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ArtifactPaths {
    pub json_path: PathBuf,
    pub csv_path: PathBuf,
}

#[cfg(feature = "remote")]
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ScenarioGraph {
    pub n: u32,
    pub r: u32,
    pub seed: u64,
    pub target_edges: usize,
    pub edges: Vec<(u32, u32, u32)>,
}

#[cfg(feature = "remote")]
#[derive(Debug, Clone, Eq, PartialEq)]
struct ScenarioRun {
    target_edges: usize,
    actual_edges: usize,
    route_entries: usize,
    missing_routes: usize,
    backup_entries: usize,
    primary_cost_sum: u64,
    graph_fingerprint: u64,
    route_fingerprint: u64,
    signature: u64,
    build_ns: u64,
}

#[cfg(feature = "remote")]
pub fn run_for_node_count(n: u32, degrees: &[u32]) -> Vec<ScenarioMetrics> {
    let mut out = Vec::with_capacity(degrees.len());
    for &r in degrees {
        out.push(run_with_replay(n, r));
    }
    out
}

#[cfg(feature = "remote")]
pub fn run_default_matrix() -> Vec<ScenarioMetrics> {
    let mut out = Vec::with_capacity(DEFAULT_NODE_COUNTS.len() * DEFAULT_DEGREES.len());
    for &n in &DEFAULT_NODE_COUNTS {
        for &r in &DEFAULT_DEGREES {
            out.push(run_with_replay(n, r));
        }
    }
    out
}

#[cfg(feature = "remote")]
pub fn scenario_graph(n: u32, r: u32) -> ScenarioGraph {
    let seed = scenario_seed(n, r);
    let (edges, target_edges) = generate_graph(n, r, seed);
    ScenarioGraph {
        n,
        r,
        seed,
        target_edges,
        edges,
    }
}

#[cfg(feature = "remote")]
pub fn graph_from_edges(edges: &[(u32, u32, u32)]) -> PeerLinkGraph<u32> {
    let mut graph = PeerLinkGraph::<u32>::new();
    for (idx, (a, b, cost)) in edges.iter().enumerate() {
        let delta = LinkDelta {
            seq: idx as u64 + 1,
            a: *a,
            b: *b,
            op: LinkDeltaOp::Up { cost: *cost },
        };
        let _ = graph.apply_delta(delta);
    }
    graph
}

pub fn emit_artifacts(stem: &str, metrics: &[ScenarioMetrics]) -> io::Result<ArtifactPaths> {
    let dir = Path::new(ARTIFACT_DIR);
    fs::create_dir_all(dir)?;

    let json_path = dir.join(format!("{stem}.json"));
    let csv_path = dir.join(format!("{stem}.csv"));

    let json = sonic_rs::to_string(metrics)
        .map_err(|e| io::Error::other(format!("json encode failed: {e}")))?;
    fs::write(&json_path, json)?;
    fs::write(&csv_path, metrics_to_csv(metrics))?;

    Ok(ArtifactPaths {
        json_path,
        csv_path,
    })
}

pub fn metrics_to_csv(metrics: &[ScenarioMetrics]) -> String {
    let mut out = String::new();
    out.push_str(
        "n,r,seed,target_edges,actual_edges,route_entries,missing_routes,backup_entries,primary_cost_sum,graph_fingerprint,route_fingerprint,replay_route_fingerprint,signature,replay_signature,replay_match,build_ns,replay_build_ns\n",
    );

    for m in metrics {
        out.push_str(&format!(
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}\n",
            m.n,
            m.r,
            m.seed,
            m.target_edges,
            m.actual_edges,
            m.route_entries,
            m.missing_routes,
            m.backup_entries,
            m.primary_cost_sum,
            m.graph_fingerprint,
            m.route_fingerprint,
            m.replay_route_fingerprint,
            m.signature,
            m.replay_signature,
            m.replay_match,
            m.build_ns,
            m.replay_build_ns
        ));
    }

    out
}

#[cfg(feature = "remote")]
fn run_with_replay(n: u32, r: u32) -> ScenarioMetrics {
    let seed = scenario_seed(n, r);
    let first = run_once(n, r, seed);
    let second = run_once(n, r, seed);

    let replay_match = first.graph_fingerprint == second.graph_fingerprint
        && first.route_fingerprint == second.route_fingerprint
        && first.signature == second.signature
        && first.route_entries == second.route_entries
        && first.missing_routes == second.missing_routes;

    ScenarioMetrics {
        n,
        r,
        seed,
        target_edges: first.target_edges,
        actual_edges: first.actual_edges,
        route_entries: first.route_entries,
        missing_routes: first.missing_routes,
        backup_entries: first.backup_entries,
        primary_cost_sum: first.primary_cost_sum,
        graph_fingerprint: first.graph_fingerprint,
        route_fingerprint: first.route_fingerprint,
        replay_route_fingerprint: second.route_fingerprint,
        signature: first.signature,
        replay_signature: second.signature,
        replay_match,
        build_ns: first.build_ns,
        replay_build_ns: second.build_ns,
    }
}

#[cfg(feature = "remote")]
fn run_once(n: u32, r: u32, seed: u64) -> ScenarioRun {
    let (edges, target_edges) = generate_graph(n, r, seed);
    let graph = graph_from_edges(&edges);

    let build_started = Instant::now();
    let routes = build_weighted_route_table(&1u32, &graph, 1);
    let build_ns = build_started.elapsed().as_nanos() as u64;

    let graph_fingerprint = fingerprint_graph(&edges);
    let (route_fingerprint, primary_cost_sum, backup_entries, route_entries) =
        fingerprint_routes(&routes);

    let missing_routes = n.saturating_sub(1).saturating_sub(route_entries as u32) as usize;

    let mut signature = FNV_OFFSET_BASIS;
    hash_u32(&mut signature, n);
    hash_u32(&mut signature, r);
    hash_usize(&mut signature, target_edges);
    hash_usize(&mut signature, edges.len());
    hash_u64(&mut signature, graph_fingerprint);
    hash_u64(&mut signature, route_fingerprint);
    hash_usize(&mut signature, route_entries);
    hash_usize(&mut signature, backup_entries);
    hash_u64(&mut signature, primary_cost_sum);

    ScenarioRun {
        target_edges,
        actual_edges: edges.len(),
        route_entries,
        missing_routes,
        backup_entries,
        primary_cost_sum,
        graph_fingerprint,
        route_fingerprint,
        signature,
        build_ns,
    }
}

#[cfg(feature = "remote")]
fn generate_graph(n: u32, r: u32, seed: u64) -> (Vec<(u32, u32, u32)>, usize) {
    assert!(n >= 3, "n must be >= 3");

    let max_edges = (n as usize) * (n as usize - 1) / 2;
    let raw_target = (n as usize) * (r as usize) / 2;
    let target_edges = raw_target.max(n as usize).min(max_edges);

    let mut rng = SplitMix64::new(seed);
    let mut undirected = HashSet::<(u32, u32)>::with_capacity(target_edges * 2);

    for a in 1..=n {
        let b = if a == n { 1 } else { a + 1 };
        undirected.insert(order_pair(a, b));
    }

    while undirected.len() < target_edges {
        let a = (rng.next_u64() % n as u64) as u32 + 1;
        let b = (rng.next_u64() % n as u64) as u32 + 1;
        if a == b {
            continue;
        }
        undirected.insert(order_pair(a, b));
    }

    let mut edges: Vec<(u32, u32, u32)> = undirected
        .into_iter()
        .map(|(a, b)| {
            let cost = deterministic_edge_cost(a, b, seed);
            (a, b, cost)
        })
        .collect();
    edges.sort_unstable_by_key(|(a, b, _)| (*a, *b));

    (edges, target_edges)
}

fn order_pair(a: u32, b: u32) -> (u32, u32) {
    if a <= b { (a, b) } else { (b, a) }
}

fn scenario_seed(n: u32, r: u32) -> u64 {
    let mut x = 0xa0761d6478bd642f_u64;
    x ^= (n as u64) << 32;
    x ^= (r as u64).wrapping_mul(0xe7037ed1a0b428db);
    mix64(x)
}

fn deterministic_edge_cost(a: u32, b: u32, seed: u64) -> u32 {
    let (lo, hi) = order_pair(a, b);
    let mixed = mix64(seed ^ ((lo as u64) << 32) ^ hi as u64);
    (mixed % 19 + 1) as u32
}

#[cfg(feature = "remote")]
fn fingerprint_graph(edges: &[(u32, u32, u32)]) -> u64 {
    let mut h = FNV_OFFSET_BASIS;
    for (a, b, c) in edges {
        hash_u32(&mut h, *a);
        hash_u32(&mut h, *b);
        hash_u32(&mut h, *c);
    }
    h
}

#[cfg(feature = "remote")]
fn fingerprint_routes(routes: &RouteTable<u32>) -> (u64, u64, usize, usize) {
    let mut rows: Vec<(u32, u32, u64, Option<u32>, Option<u64>)> = routes
        .iter()
        .map(|(dest, entry)| {
            (
                *dest,
                entry.next_hop,
                entry.cost,
                entry.backup_next_hop,
                entry.backup_cost,
            )
        })
        .collect();
    rows.sort_unstable_by_key(|(dest, _, _, _, _)| *dest);

    let mut h = FNV_OFFSET_BASIS;
    let mut cost_sum = 0u64;
    let mut backup_entries = 0usize;

    for (dest, next_hop, cost, backup_next_hop, backup_cost) in &rows {
        hash_u32(&mut h, *dest);
        hash_u32(&mut h, *next_hop);
        hash_u64(&mut h, *cost);
        match backup_next_hop {
            Some(v) => {
                hash_u32(&mut h, *v);
                backup_entries += 1;
            }
            None => hash_u32(&mut h, 0),
        }
        match backup_cost {
            Some(v) => hash_u64(&mut h, *v),
            None => hash_u64(&mut h, 0),
        }
        cost_sum = cost_sum.saturating_add(*cost);
    }

    (h, cost_sum, backup_entries, rows.len())
}

fn hash_u32(state: &mut u64, v: u32) {
    hash_bytes(state, &v.to_le_bytes());
}

fn hash_u64(state: &mut u64, v: u64) {
    hash_bytes(state, &v.to_le_bytes());
}

fn hash_usize(state: &mut u64, v: usize) {
    hash_u64(state, v as u64);
}

fn hash_bytes(state: &mut u64, bytes: &[u8]) {
    for byte in bytes {
        *state ^= *byte as u64;
        *state = state.wrapping_mul(FNV_PRIME);
    }
}

fn mix64(mut x: u64) -> u64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58476d1ce4e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}

struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e3779b97f4a7c15);
        mix64(self.state)
    }
}

#[cfg(feature = "remote")]
fn usage() -> &'static str {
    "Usage: cargo bench -p icanact-core --features remote --bench routing_graph_sim -- [options]\n\
     \n\
     Options:\n\
       --bench             Ignored (passed by cargo)\n\
       --n 100,1000        Node-count list (default: 100,1000)\n\
       --r 10,24,100       Degree list (default: 10,24,100)\n\
       --stem NAME         Output stem under artifacts/routing (default: routing_graph_sim)\n"
}

#[cfg(feature = "remote")]
fn parse_u32_list(v: &str) -> Result<Vec<u32>, String> {
    let mut out = Vec::new();
    for part in v.split(',') {
        let trimmed = part.trim();
        if trimmed.is_empty() {
            continue;
        }
        out.push(
            trimmed
                .parse::<u32>()
                .map_err(|e| format!("invalid integer '{trimmed}': {e}"))?,
        );
    }
    if out.is_empty() {
        return Err("list cannot be empty".to_string());
    }
    Ok(out)
}

#[cfg(feature = "remote")]
fn parse_args() -> Result<(Vec<(u32, u32)>, String), String> {
    let mut ns = DEFAULT_NODE_COUNTS.to_vec();
    let mut rs = DEFAULT_DEGREES.to_vec();
    let mut stem = "routing_graph_sim".to_string();

    let mut it = env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--bench" => {}
            "--help" | "-h" => return Err(usage().to_string()),
            "--n" => {
                let value = it
                    .next()
                    .ok_or_else(|| "missing value for --n".to_string())?;
                ns = parse_u32_list(&value)?;
            }
            "--r" => {
                let value = it
                    .next()
                    .ok_or_else(|| "missing value for --r".to_string())?;
                rs = parse_u32_list(&value)?;
            }
            "--stem" => {
                stem = it
                    .next()
                    .ok_or_else(|| "missing value for --stem".to_string())?;
            }
            other => return Err(format!("unknown arg: {other}\n\n{}", usage())),
        }
    }

    let mut combos = Vec::with_capacity(ns.len() * rs.len());
    for n in &ns {
        for r in &rs {
            combos.push((*n, *r));
        }
    }

    Ok((combos, stem))
}

#[cfg(feature = "remote")]
fn main() {
    let (combos, stem) = match parse_args() {
        Ok(v) => v,
        Err(msg) => {
            eprintln!("{msg}");
            return;
        }
    };

    let mut metrics = Vec::with_capacity(combos.len());
    for (n, r) in combos {
        metrics.push(run_with_replay(n, r));
    }

    match emit_artifacts(&stem, &metrics) {
        Ok(paths) => {
            println!("== routing_graph_sim ==");
            println!("json: {}", paths.json_path.display());
            println!("csv: {}", paths.csv_path.display());
            for m in &metrics {
                println!(
                    "N={} r={} edges={} routes={} replay_match={} signature={}",
                    m.n, m.r, m.actual_edges, m.route_entries, m.replay_match, m.signature
                );
            }
        }
        Err(err) => {
            eprintln!("failed to write artifacts: {err}");
        }
    }
}

#[cfg(not(feature = "remote"))]
fn main() {
    eprintln!("routing_graph_sim bench requires --features remote");
}
