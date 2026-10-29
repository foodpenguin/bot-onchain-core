//! RPC load balancing. A tower transport over several HTTP endpoints:
//! - routing: lowest healthy `priority` tier first (free/fast endpoints before quota-limited
//!   backups); inside a tier each request goes to the endpoint with the lowest
//!   latency-EWMA x (1 + in-flight requests), so concurrent requests spread across endpoints;
//! - failover: transport errors, timeouts and capacity-type JSON-RPC errors (rate limit, range
//!   limit, plan restrictions) move the request to the next endpoint and cool the failing one down;
//! - hedging: a read still unanswered after ~2x the endpoint's usual latency is also sent to the
//!   next endpoint in the same tier; the first answer wins and the other is cancelled;
//! - method routing: an endpoint never gets the methods in its `skip_methods` (configured, e.g.
//!   eth_simulateV1 on a node that refuses it, or eth_getLogs on one that is slow at it), nor ones
//!   it answered with "method not found" (learned for 10 minutes, without cooling the endpoint down);
//! - deterministic JSON-RPC errors (e.g. "execution reverted") are returned as-is.
//!
//! Endpoint URLs carry API keys: only endpoint *names* are ever logged.

use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};

use alloy::{
    rpc::json_rpc::{RequestPacket, ResponsePacket},
    transports::{TransportError, TransportErrorKind, TransportFut, http::Http},
};
use anyhow::{Context as _, Result, bail};
use tower::Service;
use tracing::warn;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(8);
const MAX_COOLDOWN: Duration = Duration::from_secs(60);
const LEARNED_SKIP_TTL: Duration = Duration::from_secs(600);
/// This many lag refusals in a row, with no success in between, and the node is not just a few
/// hundred ms behind: it lacks the blocks (pruned / non-archive on a replay, or stuck), so it is
/// cooled down like any other failure instead of being asked first every time.
const LAG_STRIKES: u32 = 5;

#[derive(Default, Debug, Clone)]
struct Stats {
    requests: u64,
    failovers: u64,
    /// Refusals because the node had not imported the block yet (also counted in failovers).
    lagging: u64,
    hedges: u64,
    consecutive_failures: u32,
    /// Lag refusals since this endpoint last served anything (see record_lag).
    consecutive_lags: u32,
    ewma_ms: f64,
    cooldown_until: Option<Instant>,
}

struct Endpoint {
    name: String,
    priority: u32,
    transport: Http<reqwest::Client>,
    stats: Mutex<Stats>,
    in_flight: AtomicUsize,
    /// Methods never routed here (configured `skip_methods`).
    skip: HashSet<String>,
    /// Methods answered with "method not found", until the given time (then re-probed: one such
    /// answer from a load-balanced provider may come from a single odd backend).
    learned: Mutex<HashMap<String, Instant>>,
    max_log_blocks: Option<u64>,
    /// Attempts per JSON-RPC method (incl. failovers and hedges: what the provider bills).
    calls: Mutex<HashMap<String, u64>>,
}

impl Endpoint {
    fn serves(&self, req: &RequestPacket) -> bool {
        if !self.skip.is_empty() && req.method_names().any(|m| self.skip.contains(m)) {
            return false;
        }
        {
            let mut learned = self.learned.lock().unwrap();
            if !learned.is_empty() {
                let now = Instant::now();
                learned.retain(|_, until| *until > now);
                if req.method_names().any(|m| learned.contains_key(m)) {
                    return false;
                }
            }
        }
        match self.max_log_blocks {
            None => true,
            Some(max) => requests(req).iter().filter(|r| r.method() == "eth_getLogs").all(|r| log_range(r.params()).is_none_or(|n| n <= max)),
        }
    }
}

fn requests(req: &RequestPacket) -> &[alloy::rpc::json_rpc::SerializedRequest] {
    match req {
        RequestPacket::Single(r) => std::slice::from_ref(r),
        RequestPacket::Batch(v) => v,
    }
}

/// Block count of an eth_getLogs filter with explicit hex bounds; None when it can't be told
/// (tags like "latest", blockHash filters): those are treated as small.
fn log_range(params: Option<&serde_json::value::RawValue>) -> Option<u64> {
    let v: serde_json::Value = serde_json::from_str(params?.get()).ok()?;
    let f = v.get(0)?;
    let num = |k: &str| f.get(k)?.as_str()?.strip_prefix("0x").and_then(|h| u64::from_str_radix(h, 16).ok());
    let (from, to) = (num("fromBlock")?, num("toBlock")?);
    Some(to.saturating_sub(from) + 1)
}

#[derive(Clone)]
pub struct RpcPool {
    endpoints: Arc<Vec<Endpoint>>,
    /// Per-attempt timeout (then the next endpoint is tried).
    timeout: Duration,
}

#[derive(Clone)]
pub struct EndpointSpec {
    pub name: String,
    pub url: String,
    pub priority: u32,
    pub skip_methods: Vec<String>,
    pub max_log_blocks: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct EndpointReport {
    pub name: String,
    pub requests: u64,
    pub failovers: u64,
    pub lagging: u64,
    pub hedges: u64,
    pub ewma_ms: f64,
    pub cooling: bool,
}

/// JSON-RPC errors that mean "this node can't/won't serve it right now", not "the call is invalid".
pub fn is_capacity_error(code: i64, message: &str) -> bool {
    let m = message.to_ascii_lowercase();
    // Deterministic outcomes of the call itself: every endpoint would say the same, so never fail
    // over (or cool an endpoint down) for them.
    // (Only the block gas limit: an endpoint's own eth_call gas cap must still fail over.)
    const DETERMINISTIC: [&str; 9] = ["revert", "invalid opcode", "out of gas", "exceeds block gas limit", "gas required exceeds", "intrinsic gas", "insufficient funds", "nonce", "fee cap"];
    if DETERMINISTIC.iter().any(|k| m.contains(k)) {
        return false;
    }
    // Phrases of rate, plan, range and availability limits, not single words ("limit" alone also
    // matches "exceeds block gas limit").
    const CAPACITY: [&str; 26] = [
        "gas cap", "gas limit too high", "rate limit", "ratelimit", "too many", "limit exceeded", "limit reached", "request limit", "compute units", "capacity",
        "block range", "range too large", "range is too large", "max range", "query returned more than", "not supported",
        "unavailable", "not available", "does not exist", "timeout", "timed out", "archive", "upgrade", "free plan", "your plan",
        "missing trie node",
    ];
    // -32601 = method not found: another endpoint may support it (e.g. eth_simulateV1).
    matches!(code, -32005 | -32016 | -32090 | -32601 | 429 | 35) || is_lagging_error(code, &m) || CAPACITY.iter().any(|k| m.contains(k))
}

/// "This endpoint does not implement the method" (so: route it elsewhere for good), as opposed
/// to a temporary or plan limit. Range limits like "... not supported on free plan" do not count.
/// "This node has not imported that block yet": blocks are announced by the flashblock stream
/// before most nodes import them, so a node a few hundred ms behind refuses state at the new head
/// (measured 2026-10-19: ankr/alchemy/infura/tenderly `-32001 block not found: 0x..`, drpc
/// `26 Unknown block`; others say `header not found`). Another endpoint usually has it: fail over,
/// but do not cool this one down. Not matching these let ~0.3% of block fetches fail outright.
pub fn is_lagging_error(_code: i64, message: &str) -> bool {
    let m = message.to_ascii_lowercase();
    ["block not found", "unknown block", "header not found"].iter().any(|k| m.contains(k))
}

pub fn is_method_unsupported(code: i64, message: &str) -> bool {
    let m = message.to_ascii_lowercase();
    code == -32601 || (m.contains("method") && (m.contains("does not exist") || m.contains("not found") || m.contains("not available")))
}

impl RpcPool {
    pub fn new(specs: Vec<EndpointSpec>) -> Result<Self> {
        Self::with_timeout(specs, REQUEST_TIMEOUT)
    }

    /// Same, with a custom per-attempt timeout (e.g. short for same-block work, where an answer
    /// later than a couple of flashblocks is worthless).
    pub fn with_timeout(specs: Vec<EndpointSpec>, timeout: Duration) -> Result<Self> {
        if specs.is_empty() {
            bail!("no RPC endpoints configured");
        }
        let http = reqwest::Client::builder().timeout(timeout).build()?;
        let endpoints = specs
            .into_iter()
            .map(|s| {
                let url = s.url.parse().with_context(|| format!("endpoint {}: bad url", s.name))?;
                Ok(Endpoint { name: s.name, priority: s.priority, transport: Http::with_client(http.clone(), url), stats: Mutex::default(), in_flight: AtomicUsize::new(0), skip: s.skip_methods.into_iter().collect(), learned: Mutex::default(), max_log_blocks: s.max_log_blocks, calls: Mutex::default() })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { endpoints: Arc::new(endpoints), timeout })
    }

    /// Endpoint indices in the order to try: healthy first, then priority tier, then
    /// load-weighted latency. Unmeasured endpoints score 0 so each gets probed early.
    fn order(&self) -> Vec<usize> {
        let now = Instant::now();
        let mut idx: Vec<(bool, u32, f64, usize)> = self
            .endpoints
            .iter()
            .enumerate()
            .map(|(i, e)| {
                let s = e.stats.lock().unwrap();
                let cooling = s.cooldown_until.is_some_and(|t| t > now);
                let load = 1.0 + e.in_flight.load(Ordering::Relaxed) as f64;
                (cooling, e.priority, s.ewma_ms * load, i)
            })
            .collect();
        idx.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)).then(a.2.total_cmp(&b.2)));
        idx.into_iter().map(|x| x.3).collect()
    }

    fn record_success(&self, i: usize, elapsed: Duration) {
        let mut s = self.endpoints[i].stats.lock().unwrap();
        s.requests += 1;
        s.consecutive_failures = 0;
        s.consecutive_lags = 0;
        s.cooldown_until = None;
        let ms = elapsed.as_secs_f64() * 1000.0;
        s.ewma_ms = if s.ewma_ms == 0.0 { ms } else { 0.8 * s.ewma_ms + 0.2 * ms };
    }

    fn record_failure(&self, i: usize, why: &str) {
        let mut s = self.endpoints[i].stats.lock().unwrap();
        s.requests += 1;
        s.failovers += 1;
        s.consecutive_failures += 1;
        let backoff = Duration::from_secs(1u64 << s.consecutive_failures.min(6)).min(MAX_COOLDOWN);
        s.cooldown_until = Some(Instant::now() + backoff);
        warn!(endpoint = %self.endpoints[i].name, why, cooldown_s = backoff.as_secs(), "rpc failover");
    }

    /// Behind the requested block: counted as a failover, no cooldown (see is_lagging_error),
    /// until LAG_STRIKES in a row: then it is a failure (cooldown).
    fn record_lag(&self, i: usize, msg: &str) {
        let mut s = self.endpoints[i].stats.lock().unwrap();
        s.lagging += 1;
        s.consecutive_lags += 1;
        if s.consecutive_lags >= LAG_STRIKES {
            drop(s);
            self.record_failure(i, msg);
            return;
        }
        s.requests += 1;
        s.failovers += 1;
    }

    /// "Method not found": stop routing that method here, without cooling the endpoint down for
    /// everything else. Only single-method requests are attributed (a batch error is ambiguous).
    fn learn_unsupported(&self, i: usize, req: &RequestPacket, why: &str) {
        let e = &self.endpoints[i];
        let mut s = e.stats.lock().unwrap();
        s.requests += 1;
        s.failovers += 1;
        drop(s);
        let methods: Vec<&str> = req.method_names().collect();
        if let [m] = methods[..] {
            if e.learned.lock().unwrap().insert(m.to_string(), Instant::now() + LEARNED_SKIP_TTL).is_none() {
                warn!(endpoint = %e.name, method = m, why, ttl_s = LEARNED_SKIP_TTL.as_secs(), "rpc method unsupported here; routing it elsewhere for a while");
            }
        } else {
            self.record_failure(i, why);
        }
    }

    /// (endpoint, method, attempts) since start, for quota accounting.
    pub fn method_report(&self) -> Vec<(String, String, u64)> {
        let mut out = vec![];
        for e in self.endpoints.iter() {
            for (m, n) in e.calls.lock().unwrap().iter() {
                out.push((e.name.clone(), m.clone(), *n));
            }
        }
        out.sort();
        out
    }

    pub fn report(&self) -> Vec<EndpointReport> {
        let now = Instant::now();
        self.endpoints
            .iter()
            .map(|e| {
                let s = e.stats.lock().unwrap();
                EndpointReport { name: e.name.clone(), requests: s.requests, failovers: s.failovers, lagging: s.lagging, hedges: s.hedges, ewma_ms: s.ewma_ms, cooling: s.cooldown_until.is_some_and(|t| t > now) }
            })
            .collect()
    }

    /// Delay before hedging a request to a second endpoint: ~2x the first endpoint's usual latency.
    fn hedge_delay(&self, i: usize) -> Duration {
        let ewma = self.endpoints[i].stats.lock().unwrap().ewma_ms;
        if ewma == 0.0 { Duration::from_millis(400) } else { Duration::from_millis((2.0 * ewma).clamp(120.0, 1500.0) as u64) }
    }

    async fn attempt(&self, i: usize, req: RequestPacket) -> (usize, Duration, Attempt) {
        /// Keeps the in-flight counter right even when a losing hedge is cancelled mid-request.
        struct InFlight<'a>(&'a AtomicUsize);
        impl Drop for InFlight<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::Relaxed);
            }
        }
        let e = &self.endpoints[i];
        e.in_flight.fetch_add(1, Ordering::Relaxed);
        {
            let mut calls = e.calls.lock().unwrap();
            for m in req.method_names() {
                *calls.entry(m.to_string()).or_default() += 1;
            }
        }
        let _guard = InFlight(&e.in_flight);
        let start = Instant::now();
        let mut t = e.transport.clone();
        let outcome = match tokio::time::timeout(self.timeout, t.call(req)).await {
            Ok(Ok(resp)) => {
                if resp.iter_errors().any(|er| is_capacity_error(er.code, &er.message)) {
                    let kind = if resp.iter_errors().any(|er| is_method_unsupported(er.code, &er.message)) {
                        Refusal::Unsupported
                    } else if resp.iter_errors().all(|er| is_lagging_error(er.code, &er.message)) {
                        Refusal::Lagging
                    } else {
                        Refusal::Capacity
                    };
                    let msg = resp.first_error_message().unwrap_or_default().chars().take(80).collect::<String>();
                    Attempt::Refused(resp, msg, kind)
                } else {
                    Attempt::Ok(resp)
                }
            }
            // The original error text embeds the URL (with its API key): replace it.
            Ok(Err(_)) => Attempt::Failed(TransportErrorKind::custom_str(&format!("transport error on rpc endpoint {}", e.name)), "transport error"),
            Err(_) => Attempt::Failed(TransportErrorKind::custom_str(&format!("timeout on rpc endpoint {}", e.name)), "timeout"),
        };
        (i, start.elapsed(), outcome)
    }

    async fn request(&self, req: RequestPacket) -> Result<ResponsePacket, TransportError> {
        use futures::{StreamExt, stream::FuturesUnordered};
        // Only endpoints that serve every method in the request; if none does, try them all anyway.
        let all = self.order();
        let served: Vec<usize> = all.iter().copied().filter(|&i| self.endpoints[i].serves(&req)).collect();
        let order = if served.is_empty() { all } else { served };
        // Sends are not hedged (no duplicate submission races); they still fail over in order.
        let hedgeable = !req.method_names().any(|m| m.starts_with("eth_send"));
        let tier = self.endpoints[order[0]].priority;
        let mut pending = FuturesUnordered::new();
        pending.push(self.attempt(order[0], req.clone()));
        let mut next = 1;
        let mut hedged = false;
        let hedge_timer = tokio::time::sleep(self.hedge_delay(order[0]));
        tokio::pin!(hedge_timer);
        let (mut last_resp, mut last_err) = (None, None);
        loop {
            tokio::select! {
                Some((i, elapsed, outcome)) = pending.next() => {
                    match outcome {
                        Attempt::Ok(resp) => {
                            self.record_success(i, elapsed);
                            return Ok(resp); // dropping `pending` cancels a slower hedge
                        }
                        Attempt::Refused(resp, msg, Refusal::Unsupported) => {
                            self.learn_unsupported(i, &req, &msg);
                            last_resp = Some(resp);
                        }
                        Attempt::Refused(resp, msg, Refusal::Lagging) => {
                            self.record_lag(i, &msg);
                            last_resp = Some(resp);
                        }
                        Attempt::Refused(resp, msg, Refusal::Capacity) => {
                            self.record_failure(i, &msg);
                            last_resp = Some(resp);
                        }
                        Attempt::Failed(err, why) => {
                            self.record_failure(i, why);
                            last_err = Some(err);
                        }
                    }
                    if pending.is_empty() {
                        if next >= order.len() {
                            break;
                        }
                        pending.push(self.attempt(order[next], req.clone()));
                        next += 1;
                    }
                }
                _ = &mut hedge_timer, if hedgeable && !hedged && next < order.len() && self.endpoints[order[next]].priority == tier => {
                    hedged = true;
                    self.endpoints[order[next]].stats.lock().unwrap().hedges += 1;
                    pending.push(self.attempt(order[next], req.clone()));
                    next += 1;
                }
                else => break,
            }
        }
        // Every endpoint refused: surface the JSON-RPC error if we have one, else the transport error.
        match (last_resp, last_err) {
            (Some(r), _) => Ok(r),
            (None, Some(e)) => Err(e),
            (None, None) => Err(TransportErrorKind::custom_str("no rpc endpoints")),
        }
    }
}

enum Attempt {
    Ok(ResponsePacket),
    /// Node answered with a capacity-type JSON-RPC error (rate/range/plan limit, missing block).
    Refused(ResponsePacket, String, Refusal),
    Failed(TransportError, &'static str),
}

/// What a refusal says about the endpoint; every kind moves the request to the next one.
enum Refusal {
    /// "Method not found": learned per method, no endpoint cooldown.
    Unsupported,
    /// Has not imported the block yet: no cooldown, it catches up within a few hundred ms.
    Lagging,
    /// Rate / range / plan limit: the endpoint is cooled down.
    Capacity,
}

impl Service<RequestPacket> for RpcPool {
    type Response = ResponsePacket;
    type Error = TransportError;
    type Future = TransportFut<'static>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: RequestPacket) -> Self::Future {
        let this = self.clone();
        Box::pin(async move { this.request(req).await })
    }
}

/// Minimal .env reader (KEY=VALUE, optional quotes, # comments). Values are secrets: never log them.
pub fn load_env_file(path: &str) -> Result<HashMap<String, String>> {
    let text = std::fs::read_to_string(path).with_context(|| format!("reading env file {path}"))?;
    Ok(text
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .filter_map(|l| l.split_once('='))
        .map(|(k, v)| (k.trim().to_string(), v.trim().trim_matches('"').trim_matches('\'').to_string()))
        .collect())
}

/// WebSocket URL for a provider's HTTP URL (Infura uses /ws/v3/, most others just swap the scheme).
pub fn ws_from_http(url: &str) -> Option<String> {
    let rest = url.strip_prefix("https://")?;
    if rest.contains("infura.io/v3/") {
        return Some(format!("wss://{}", rest.replacen("/v3/", "/ws/v3/", 1)));
    }
    Some(format!("wss://{rest}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capacity_errors_fail_over_deterministic_ones_do_not() {
        for m in ["You reached free plan rate limit, please upgrade", "request limit reached", "ranges over 10000 blocks are not supported on freetier", "Under the Free tier plan, you can make eth_getLogs requests with up to a 10 block range", "query returned more than 10000 results", "upstream timed out"] {
            assert!(is_capacity_error(-32000, m), "{m}");
        }
        for m in ["exceeds block gas limit", "gas required exceeds allowance (30000000)", "insufficient funds for gas * price + value", "nonce too low", "execution reverted: InsufficientProfit", "max fee per gas less than block base fee: fee cap"] {
            assert!(!is_capacity_error(-32000, m), "{m}");
        }
        assert!(is_capacity_error(-32000, "gas limit too high: this node caps eth_call gas"), "a node's own cap: another may serve it");
        assert!(is_capacity_error(429, "Too Many Requests"));
    }

    #[test]
    fn capacity_errors() {
        assert!(is_capacity_error(35, "ranges over 10000 blocks are not supported on free plan"));
        assert!(is_capacity_error(-32005, "limit exceeded"));
        assert!(is_capacity_error(429, "Too Many Requests"));
        assert!(is_capacity_error(-32000, "header not found"));
        assert!(is_capacity_error(-32601, "The method eth_simulateV1 does not exist/is not available"));
        assert!(!is_capacity_error(3, "execution reverted: Unauthorized"));
        assert!(!is_capacity_error(-32000, "execution reverted"));
        assert!(!is_capacity_error(-32602, "invalid argument 0: hex string without 0x prefix"));
    }

    #[test]
    fn method_unsupported_vs_plan_limits() {
        assert!(is_method_unsupported(-32601, "method not found"));
        assert!(is_method_unsupported(-32000, "The method eth_simulateV1 does not exist/is not available"));
        // Plan/range/rate limits are temporary or size-specific: they must not exclude the method.
        assert!(!is_method_unsupported(35, "ranges over 10000 blocks are not supported on free plan"));
        assert!(!is_method_unsupported(-32005, "rate limit exceeded"));
        assert!(!is_method_unsupported(429, "Too Many Requests"));
    }

    #[test]
    fn log_range_parsing() {
        let raw = |s: &str| serde_json::value::RawValue::from_string(s.to_string()).unwrap();
        assert_eq!(log_range(Some(&raw(r#"[{"fromBlock":"0x10","toBlock":"0x10"}]"#))), Some(1));
        assert_eq!(log_range(Some(&raw(r#"[{"fromBlock":"0x1","toBlock":"0x64","address":"0xabc"}]"#))), Some(100));
        assert_eq!(log_range(Some(&raw(r#"[{"fromBlock":"latest","toBlock":"latest"}]"#))), None);
        assert_eq!(log_range(Some(&raw(r#"[{"blockHash":"0x12"}]"#))), None);
        assert_eq!(log_range(None), None);
    }

    #[tokio::test]
    async fn wide_get_logs_skip_range_limited_endpoint() {
        // Endpoint 0 only takes <=10-block getLogs (Alchemy free plan): a 100-block query must go
        // straight to endpoint 1 without being refused (and cooled down) at endpoint 0 first.
        let pool = RpcPool::new(vec![
            EndpointSpec { name: "small".into(), url: "http://127.0.0.1:9".into(), priority: 0, skip_methods: vec![], max_log_blocks: Some(10) },
            EndpointSpec { name: "public".into(), url: "https://base.gateway.tenderly.co".into(), priority: 1, skip_methods: vec![], max_log_blocks: None },
        ])
        .unwrap();
        use alloy::{
            providers::{Provider, ProviderBuilder},
            rpc::types::Filter,
        };
        let provider = ProviderBuilder::new().connect_client(alloy::rpc::client::RpcClient::new(pool.clone(), false));
        let head = RpcPool::new(vec![EndpointSpec { name: "p".into(), url: "https://base.gateway.tenderly.co".into(), priority: 0, skip_methods: vec![], max_log_blocks: None }]).unwrap();
        let head = ProviderBuilder::new().connect_client(alloy::rpc::client::RpcClient::new(head, false)).get_block_number().await.unwrap();
        let filter = Filter::new().address(alloy::primitives::address!("BBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb")).from_block(head - 99).to_block(head);
        provider.get_logs(&filter).await.expect("wide query served by the unlimited endpoint");
        let r = pool.report();
        assert_eq!((r[0].requests, r[0].cooling), (0, false));
        assert_eq!(r[1].requests, 1);
    }

    #[tokio::test]
    async fn skip_methods_route_around_endpoint() {
        // Endpoint 0 is preferred but configured not to serve eth_blockNumber: the request must go
        // straight to endpoint 1, and endpoint 0 must neither be tried nor cooled down.
        let pool = RpcPool::new(vec![
            EndpointSpec { name: "skipper".into(), url: "http://127.0.0.1:9".into(), priority: 0, skip_methods: vec!["eth_blockNumber".into()], max_log_blocks: None },
            EndpointSpec { name: "public".into(), url: "https://base.gateway.tenderly.co".into(), priority: 1, skip_methods: vec![], max_log_blocks: None },
        ])
        .unwrap();
        use alloy::providers::{Provider, ProviderBuilder};
        let provider = ProviderBuilder::new().connect_client(alloy::rpc::client::RpcClient::new(pool.clone(), false));
        assert!(provider.get_block_number().await.unwrap() > 50_000_000);
        let r = pool.report();
        assert_eq!((r[0].requests, r[0].failovers, r[0].cooling), (0, 0, false));
        assert_eq!(r[1].requests, 1);
    }

    #[test]
    fn learned_skips_expire() {
        let pool = RpcPool::new(vec![EndpointSpec { name: "a".into(), url: "http://127.0.0.1:1".into(), priority: 0, skip_methods: vec![], max_log_blocks: None }]).unwrap();
        let req: RequestPacket = alloy::rpc::json_rpc::Request::new("eth_simulateV1", alloy::rpc::json_rpc::Id::Number(1), ()).serialize().unwrap().into();
        let e = &pool.endpoints[0];
        e.learned.lock().unwrap().insert("eth_simulateV1".into(), Instant::now() + Duration::from_secs(60));
        assert!(!e.serves(&req), "still within the TTL");
        e.learned.lock().unwrap().insert("eth_simulateV1".into(), Instant::now() - Duration::from_secs(1));
        assert!(e.serves(&req), "expired: probe the method again");
        assert!(e.learned.lock().unwrap().is_empty(), "expired entries are pruned");
    }

    #[tokio::test]
    async fn method_not_found_is_learned_without_cooldown() {
        // A "method not found" answer excludes only that method; the endpoint keeps serving the rest.
        let pool = RpcPool::new(vec![EndpointSpec { name: "public".into(), url: "https://base.gateway.tenderly.co".into(), priority: 0, skip_methods: vec![], max_log_blocks: None }]).unwrap();
        use alloy::providers::{Provider, ProviderBuilder};
        let provider = ProviderBuilder::new().connect_client(alloy::rpc::client::RpcClient::new(pool.clone(), false));
        let err = provider.raw_request::<_, serde_json::Value>("eth_noSuchMethodXyz".into(), ()).await;
        assert!(err.is_err());
        assert!(pool.endpoints[0].learned.lock().unwrap().contains_key("eth_noSuchMethodXyz"));
        assert!(!pool.report()[0].cooling, "must not cool the endpoint down for one unsupported method");
        assert!(provider.get_block_number().await.unwrap() > 50_000_000);
    }

    #[test]
    fn ws_derivation() {
        assert_eq!(ws_from_http("https://base-mainnet.infura.io/v3/KEY").unwrap(), "wss://base-mainnet.infura.io/ws/v3/KEY");
        assert_eq!(ws_from_http("https://lb.drpc.live/base/KEY").unwrap(), "wss://lb.drpc.live/base/KEY");
        assert!(ws_from_http("http://localhost:8545").is_none());
    }

    #[test]
    fn in_flight_spreads_load_within_tier() {
        let pool = RpcPool::new(vec![
            EndpointSpec { name: "a".into(), url: "http://127.0.0.1:1".into(), priority: 0, skip_methods: vec![], max_log_blocks: None },
            EndpointSpec { name: "b".into(), url: "http://127.0.0.1:2".into(), priority: 0, skip_methods: vec![], max_log_blocks: None },
            EndpointSpec { name: "backup".into(), url: "http://127.0.0.1:3".into(), priority: 1, skip_methods: vec![], max_log_blocks: None },
        ])
        .unwrap();
        pool.record_success(0, Duration::from_millis(150));
        pool.record_success(1, Duration::from_millis(250));
        pool.record_success(2, Duration::from_millis(50));
        // Faster endpoint first; the cheaper-but-faster backup tier never jumps ahead.
        assert_eq!(pool.order(), vec![0, 1, 2]);
        // Two requests already in flight on `a` (150*3=450 > 250): the next one goes to `b`.
        pool.endpoints[0].in_flight.store(2, Ordering::Relaxed);
        assert_eq!(pool.order()[0], 1);
    }

    /// Minimal JSON-RPC server: answers every request with `result` after `delay`.
    async fn mock_rpc(delay: Duration, result: &'static str) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else { return };
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 65536];
                    let n = sock.read(&mut buf).await.unwrap_or(0);
                    let text = String::from_utf8_lossy(&buf[..n]);
                    let body = text.split("\r\n\r\n").nth(1).unwrap_or("{}");
                    let id = serde_json::from_str::<serde_json::Value>(body).ok().map(|v| v["id"].clone()).unwrap_or(0.into());
                    tokio::time::sleep(delay).await;
                    let payload = format!(r#"{{"jsonrpc":"2.0","id":{id},"result":"{result}"}}"#);
                    let resp = format!("HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{payload}", payload.len());
                    let _ = sock.write_all(resp.as_bytes()).await;
                });
            }
        });
        format!("http://{addr}")
    }

    /// Minimal JSON-RPC server that answers every request with a JSON-RPC error.
    async fn mock_rpc_error(code: i64, message: &'static str) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else { return };
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 65536];
                    let n = sock.read(&mut buf).await.unwrap_or(0);
                    let text = String::from_utf8_lossy(&buf[..n]);
                    let body = text.split("\r\n\r\n").nth(1).unwrap_or("{}");
                    let id = serde_json::from_str::<serde_json::Value>(body).ok().map(|v| v["id"].clone()).unwrap_or(0.into());
                    let payload = format!(r#"{{"jsonrpc":"2.0","id":{id},"error":{{"code":{code},"message":"{message}"}}}}"#);
                    let resp = format!("HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{payload}", payload.len());
                    let _ = sock.write_all(resp.as_bytes()).await;
                });
            }
        });
        format!("http://{addr}")
    }

    #[test]
    fn lagging_errors() {
        // 2026-10-19 answers for a block one past the node's head.
        assert!(is_lagging_error(-32001, "block not found: 0x325caf8"));
        assert!(is_lagging_error(26, "Unknown block"));
        assert!(is_lagging_error(-32000, "header not found"));
        assert!(is_capacity_error(-32001, "block not found: 0x325caf8"), "must fail over");
        assert!(!is_lagging_error(3, "execution reverted"));
        assert!(!is_lagging_error(-32005, "rate limit exceeded"));
    }

    #[tokio::test]
    async fn lagging_node_fails_over_without_cooldown() {
        // Preferred endpoint has not imported the block yet: the request must be served by the
        // next one, and the lagging endpoint must stay first in line (it is not unhealthy).
        let behind = mock_rpc_error(-32001, "block not found: 0x325caf8").await;
        let synced = mock_rpc(Duration::from_millis(5), "0x2").await;
        let pool = RpcPool::new(vec![
            EndpointSpec { name: "behind".into(), url: behind, priority: 0, skip_methods: vec![], max_log_blocks: None },
            EndpointSpec { name: "synced".into(), url: synced, priority: 1, skip_methods: vec![], max_log_blocks: None },
        ])
        .unwrap();
        use alloy::providers::{Provider, ProviderBuilder};
        let provider = ProviderBuilder::new().connect_client(alloy::rpc::client::RpcClient::new(pool.clone(), false));
        let out: String = provider.raw_request("eth_call".into(), ()).await.expect("served by the synced endpoint");
        assert_eq!(out, "0x2");
        let r = pool.report();
        assert_eq!((r[0].failovers, r[0].lagging, r[0].cooling), (1, 1, false));
        assert_eq!(pool.order()[0], 0, "no cooldown for a node that is only behind");
        // ... but one that never has the block (pruned, on a replay) is cooled after LAG_STRIKES.
        for _ in 1..LAG_STRIKES {
            let _: String = provider.raw_request("eth_call".into(), ()).await.unwrap();
        }
        let r = pool.report();
        assert_eq!((r[0].lagging, r[0].cooling), (LAG_STRIKES as u64, true));
        assert_eq!(pool.order()[0], 1);
    }

    #[tokio::test]
    async fn hedge_beats_slow_endpoint() {
        let slow = mock_rpc(Duration::from_millis(2000), "0x1").await;
        let fast = mock_rpc(Duration::from_millis(20), "0x2").await;
        let pool = RpcPool::new(vec![
            EndpointSpec { name: "slow".into(), url: slow, priority: 0, skip_methods: vec![], max_log_blocks: None },
            EndpointSpec { name: "fast".into(), url: fast, priority: 0, skip_methods: vec![], max_log_blocks: None },
        ])
        .unwrap();
        // History says `slow` is usually 100 ms, so it is tried first and hedged after ~200 ms.
        pool.record_success(0, Duration::from_millis(100));
        pool.record_success(1, Duration::from_millis(300));
        use alloy::providers::{Provider, ProviderBuilder};
        let provider = ProviderBuilder::new().connect_client(alloy::rpc::client::RpcClient::new(pool.clone(), false));
        let t = Instant::now();
        let n = provider.get_block_number().await.unwrap();
        assert_eq!(n, 2, "hedged endpoint's answer should win");
        assert!(t.elapsed() < Duration::from_millis(1000), "took {:?}", t.elapsed());
        assert_eq!(pool.report()[1].hedges, 1);
        // The cancelled slow attempt must not leak in-flight load.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(pool.endpoints[0].in_flight.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn failover_on_dead_endpoint() {
        // Endpoint 0 is unreachable (priority 0); the pool must fall over to endpoint 1 and
        // cool endpoint 0 down so the next request goes straight to the healthy one.
        let pool = RpcPool::new(vec![
            EndpointSpec { name: "dead".into(), url: "http://127.0.0.1:9".into(), priority: 0, skip_methods: vec![], max_log_blocks: None },
            EndpointSpec { name: "public".into(), url: "https://base.gateway.tenderly.co".into(), priority: 1, skip_methods: vec![], max_log_blocks: None },
        ])
        .unwrap();
        use alloy::providers::{Provider, ProviderBuilder};
        let provider = ProviderBuilder::new().connect_client(alloy::rpc::client::RpcClient::new(pool.clone(), false));
        let n = provider.get_block_number().await.expect("failover should succeed");
        assert!(n > 50_000_000);
        assert_eq!(pool.order()[0], 1, "dead endpoint should be cooling down");
        let r = pool.report();
        assert_eq!(r[0].failovers, 1);
        assert!(r[0].cooling);
    }

    #[tokio::test]
    async fn errors_never_contain_urls() {
        let secret = "http://127.0.0.1:9/secret-api-key-123";
        let pool = RpcPool::new(vec![EndpointSpec { name: "only".into(), url: secret.into(), priority: 0, skip_methods: vec![], max_log_blocks: None }]).unwrap();
        use alloy::providers::{Provider, ProviderBuilder};
        let provider = ProviderBuilder::new().connect_client(alloy::rpc::client::RpcClient::new(pool, false));
        let err = provider.get_block_number().await.unwrap_err().to_string();
        assert!(!err.contains("secret-api-key"), "leaked: {err}");
        assert!(err.contains("only"));
    }
}
