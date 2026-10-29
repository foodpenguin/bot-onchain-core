//! Live execution: signs transactions with the hot wallet and broadcasts them.
//!
//! Safety rails (all enforced here or by the caller before `send`):
//! - the key only ever comes from an encrypted keystore; the password from an env var (removed
//!   from the process environment right after reading) or an interactive no-echo prompt;
//! - up to `max_inflight` txs in flight (consecutive nonces), never two for the same `TxKey`;
//!   one given up on while still pending is replaced by a 0-value self-transfer (cancel);
//! - nonce kept locally, resynced from the chain after drops or nonce errors; with a shared nonce
//!   file (several bots on one wallet) nothing is sent unless its lock is held;
//! - the raw tx goes to every endpoint at once (RPC pool + sequencer); endpoint URLs carry API
//!   keys, so only endpoint names are logged;
//! - failed (reverted) or dropped txs are counted in a window (`FailurePolicy`): a burst pauses
//!   sending for a cooldown, after which it resumes by itself.

use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use alloy::{
    eips::eip2718::Encodable2718,
    network::{Ethereum, EthereumWallet, NetworkTransactionBuilder, TransactionBuilder},
    primitives::{Address, B256, Bytes, U256},
    providers::Provider,
    rpc::types::TransactionRequest,
    signers::local::PrivateKeySigner,
};
use anyhow::{Context, Result, bail};
use futures::{StreamExt, future::join_all, stream::FuturesUnordered};
use serde::Serialize;
use serde_json::{Value, json};
use tracing::{info, warn};

/// Where in the calldata the minProfit field lives (patched at send time).
#[derive(Debug, Clone, Copy)]
pub struct MinProfitSlot {
    pub offset: usize,
    pub width: usize,
}


/// What the planner hands over for sending.
#[derive(Debug, Clone)]
pub struct Payload {
    pub to: Address,
    pub calldata: Bytes,
    pub min_profit: MinProfitSlot,
}

/// A broadcast error that means our nonce was taken: already mined ("nonce too low: next nonce 40,
/// tx nonce 39"; infura: "nonce=39 minNonce=40 ..."), or held by another pending tx that pays more
/// ("replacement transaction underpriced").
fn nonce_conflict(err: &str) -> bool {
    let e = err.to_ascii_lowercase();
    e.contains("nonce too low") || e.contains("minnonce") || e.contains("replacement transaction underpriced") || e.contains("replacement underpriced")
}

/// The hot wallet's next nonce as last used by any process signing for it (several bots may sign
/// for one wallet), in a file all of them lock around sign + broadcast. Without it two sends in the
/// same seconds could take the same nonce, and a bid (more fee) replaced the other in the mempool.
/// An entry older than SHARED_NONCE_TTL is ignored: the tx behind it may have been dropped.
///
/// File format (one line): `v1 <next nonce> <unix ms>`. Every bot built on this crate reads and
/// writes the same format; a line it does not recognise (another version) is never trusted: the
/// sender then refuses to send rather than guess a nonce (see `Executor::send`). The one exception
/// is a stale line in the unversioned format of earlier releases, which is treated as empty.
pub struct SharedNonce {
    path: std::path::PathBuf,
}

/// The shared nonce file's format tag.
pub const SHARED_NONCE_VERSION: &str = "v1";

/// What a shared nonce file holds.
#[derive(Debug, PartialEq, Eq)]
pub enum SharedEntry {
    /// Empty file, or an entry older than SHARED_NONCE_TTL.
    None,
    Next(u64),
    /// Written in a format this version does not know.
    Unknown,
}

const SHARED_NONCE_TTL: Duration = Duration::from_secs(30);
/// Longer than a send holds the lock (sign + a broadcast that can take BROADCAST_TIMEOUT): a
/// shorter wait gave up while the other process was still broadcasting, and sent unlocked.
const SHARED_NONCE_WAIT: Duration = Duration::from_secs(BROADCAST_TIMEOUT.as_secs() + 2);

impl SharedNonce {
    async fn lock(&self) -> Result<std::fs::File> {
        let f = std::fs::OpenOptions::new().read(true).write(true).create(true).truncate(false).open(&self.path).with_context(|| format!("opening {}", self.path.display()))?;
        let t0 = Instant::now();
        loop {
            match f.try_lock() {
                Ok(()) => return Ok(f),
                Err(std::fs::TryLockError::WouldBlock) if t0.elapsed() < SHARED_NONCE_WAIT => tokio::time::sleep(Duration::from_millis(5)).await,
                Err(e) => bail!("locking {}: {e}", self.path.display()),
            }
        }
    }

    fn read(f: &mut std::fs::File) -> SharedEntry {
        use std::io::{Read, Seek};
        let mut s = String::new();
        if f.seek(std::io::SeekFrom::Start(0)).is_err() || f.read_to_string(&mut s).is_err() {
            return SharedEntry::Unknown;
        }
        Self::parse(&s, unix_ms())
    }

    /// `v1 <next> <unix ms>`, fresh within SHARED_NONCE_TTL of `now_ms`.
    pub fn parse(s: &str, now_ms: u64) -> SharedEntry {
        let s = s.trim();
        if s.is_empty() {
            return SharedEntry::None;
        }
        let fresh = |ms: u64| now_ms.saturating_sub(ms) < SHARED_NONCE_TTL.as_millis() as u64;
        let f: Vec<&str> = s.split_whitespace().collect();
        let num = |i: usize| f.get(i).and_then(|x| x.parse::<u64>().ok());
        match (f.first().copied(), f.len()) {
            (Some(SHARED_NONCE_VERSION), 3) => match (num(1), num(2)) {
                (Some(next), Some(ms)) if fresh(ms) => SharedEntry::Next(next),
                (Some(_), Some(_)) => SharedEntry::None,
                _ => SharedEntry::Unknown,
            },
            // The unversioned `<next> <unix ms>` of earlier releases: a stale entry says nothing
            // about a tx in flight and is overwritten; a fresh one means an older bot may be
            // sending right now, so it is not trusted.
            (_, 2) => match (num(0), num(1)) {
                (Some(_), Some(ms)) if !fresh(ms) => SharedEntry::None,
                _ => SharedEntry::Unknown,
            },
            _ => SharedEntry::Unknown,
        }
    }

    fn write(f: &mut std::fs::File, next: u64) {
        use std::io::{Seek, Write};
        let ok = f.set_len(0).is_ok() && f.seek(std::io::SeekFrom::Start(0)).is_ok() && write!(f, "{SHARED_NONCE_VERSION} {next} {}", unix_ms()).is_ok();
        if !ok {
            warn!("shared nonce file write failed");
        }
    }
}

fn unix_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_millis() as u64)
}

/// Writes `value` big-endian into the minProfit field.
pub fn patch_min_profit(calldata: &Bytes, slot: MinProfitSlot, value: U256) -> Result<Bytes> {
    let mut d = calldata.to_vec();
    if d.len() < slot.offset + slot.width {
        bail!("calldata too short for minProfit patch");
    }
    let be = value.to_be_bytes::<32>();
    if be[..32 - slot.width].iter().any(|b| *b != 0) {
        bail!("minProfit does not fit in {} bytes", slot.width);
    }
    d[slot.offset..slot.offset + slot.width].copy_from_slice(&be[32 - slot.width..]);
    Ok(d.into())
}

/// Reads the keystore password from `password_env` (then removes it from this process'
/// environment) or, if unset, from a no-echo prompt.
///
/// MUST run before any other thread exists (i.e. before the tokio runtime starts): mutating the
/// environment while another thread may read it is undefined behaviour.
pub fn read_password_pre_runtime(password_env: &str, keystore: &str) -> Result<String> {
    match std::env::var(password_env) {
        Ok(p) => {
            // SAFETY: called from main() before the async runtime or any other thread is created,
            // so no concurrent getenv can observe the environment mid-update.
            unsafe { std::env::remove_var(password_env) };
            Ok(p)
        }
        Err(_) => rpassword::prompt_password(format!("keystore password for {keystore}: ")).context("reading password"),
    }
}

/// Decrypts the hot-wallet key. Never logs or returns the password.
pub fn decrypt_signer(keystore: &str, password: &str) -> Result<PrivateKeySigner> {
    PrivateKeySigner::decrypt_keystore(keystore, password).map_err(|_| anyhow::anyhow!("could not decrypt keystore {keystore} (wrong password?)"))
}

/// Pre-runtime entry: password (env or prompt) + decrypt.
pub fn load_signer(keystore: &str, password_env: &str) -> Result<PrivateKeySigner> {
    let password = read_password_pre_runtime(password_env, keystore)?;
    decrypt_signer(keystore, &password)
}

/// Per-request timeout of the broadcast client.
const BROADCAST_TIMEOUT: Duration = Duration::from_secs(10);
/// How often Broadcaster::spawn_keepalive touches each endpoint.
pub const KEEPALIVE_EVERY: Duration = Duration::from_secs(20);
/// An endpoint that rate-limits a keepalive is skipped this long.
const KEEPALIVE_BACKOFF: Duration = Duration::from_secs(300);

pub struct Broadcaster {
    http: reqwest::Client,
    /// (name, url) — url may embed an API key: never log it.
    endpoints: Vec<(String, String)>,
}

impl Broadcaster {
    pub fn new(http: reqwest::Client, endpoints: Vec<(String, String)>) -> Self {
        Self { http, endpoints }
    }

    /// HTTP client for sending: pooled connections never expire and TCP keepalive is on, so (with
    /// spawn_keepalive) a send rides an open connection. A cold one pays DNS + TCP + TLS, which was
    /// measured at 2.5-7 s for a first send after hours idle (WSL2 host, slow first DNS lookup).
    pub fn client() -> Result<reqwest::Client> {
        reqwest::Client::builder()
            .pool_idle_timeout(None)
            .tcp_keepalive(Duration::from_secs(15))
            .tcp_nodelay(true)
            .connect_timeout(Duration::from_secs(5))
            .timeout(BROADCAST_TIMEOUT)
            .build()
            .context("building the broadcast HTTP client")
    }

    /// Keeps every endpoint's connection warm: an eth_chainId each `every` (servers close idle
    /// keep-alive connections after ~60 s or more). Any HTTP answer keeps it open, an error too
    /// (the sequencer answers 403 "rpc method is not allowed" on HTTP/2, connection kept). An
    /// endpoint that answers with a rate limit is left alone for KEEPALIVE_BACKOFF: the same IP's
    /// reads share its quota (tenderly-public).
    pub fn spawn_keepalive(&self, every: Duration) {
        let (http, endpoints) = (self.http.clone(), self.endpoints.clone());
        tokio::spawn(async move {
            let body = json!({"jsonrpc": "2.0", "id": 1, "method": "eth_chainId", "params": []});
            let mut resume: Vec<Option<tokio::time::Instant>> = vec![None; endpoints.len()];
            let mut t = tokio::time::interval(every);
            t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                t.tick().await;
                let now = tokio::time::Instant::now();
                let (http, body) = (&http, &body);
                let limited = join_all(endpoints.iter().zip(&resume).map(|((_, url), r)| async move {
                    if r.is_some_and(|r| now < r) {
                        return false;
                    }
                    let Ok(resp) = http.post(url).json(body).send().await else { return false };
                    let status = resp.status();
                    // The body is read so that the connection goes back to the pool.
                    let text = resp.text().await.unwrap_or_default().to_ascii_lowercase();
                    status == reqwest::StatusCode::TOO_MANY_REQUESTS || text.contains("rate limit") || text.contains("limit reached")
                }))
                .await;
                for (r, hit) in resume.iter_mut().zip(limited) {
                    if hit {
                        *r = Some(now + KEEPALIVE_BACKOFF);
                    }
                }
            }
        });
    }

    /// Sends the raw tx to all endpoints concurrently and returns the tx hash as soon as one
    /// accepts it (or reports it as already known); the others finish in the background, only to
    /// be logged (a slow endpoint took 5-7 s and held up the caller, 2026-10-22).
    pub async fn send_raw(&self, raw: &[u8], expected_hash: B256) -> Result<B256> {
        let hex = format!("0x{}", alloy::hex::encode(raw));
        let body = json!({"jsonrpc": "2.0", "id": 1, "method": "eth_sendRawTransaction", "params": [hex]});
        let mut sends: FuturesUnordered<_> = self
            .endpoints
            .iter()
            .map(|(name, url)| {
                let (http, name, url, body) = (self.http.clone(), name.clone(), url.clone(), body.clone());
                async move {
                    let t = Instant::now();
                    let r = http.post(&url).json(&body).send().await;
                    let ms = t.elapsed().as_millis() as u64;
                    match r {
                        Ok(resp) => match resp.json::<Value>().await {
                            Ok(v) if v.get("result").is_some() => (name, true, ms, String::new()),
                            Ok(v) => {
                                let msg = v["error"]["message"].as_str().unwrap_or("unknown").to_string();
                                let known = msg.contains("already known") || msg.contains("known transaction");
                                (name, known, ms, msg)
                            }
                            Err(_) => (name, false, ms, "bad response".into()),
                        },
                        Err(_) => (name, false, ms, "transport error".into()),
                    }
                }
            })
            .collect();
        let log = |(name, ok, ms, msg): &(String, bool, u64, String)| {
            if *ok {
                info!(endpoint = %name, ms, "broadcast accepted");
            } else {
                warn!(endpoint = %name, ms, error = %msg.chars().take(100).collect::<String>(), "broadcast rejected");
            }
        };
        let mut errors = Vec::new();
        while let Some(r) = sends.next().await {
            log(&r);
            if r.1 {
                tokio::spawn(async move {
                    while let Some(r) = sends.next().await {
                        log(&r);
                    }
                });
                return Ok(expected_hash);
            }
            errors.push(r.3);
        }
        bail!("no endpoint accepted the tx: {}", errors.join(" | "))
    }
}

/// What a tx is about, for the in-flight rules: `group` is the state it competes on (a market, a
/// set of pools), `id` the item within it (a position, an opportunity). Two txs with the same key
/// are never in flight together; callers decide whether a group may have more than one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
pub struct TxKey {
    pub group: u64,
    pub id: B256,
}

impl TxKey {
    pub fn new(group: u64, id: B256) -> Self {
        Self { group, id }
    }
    /// An item named by an address (a borrower, a pool).
    pub fn of_address(group: u64, a: Address) -> Self {
        Self { group, id: a.into_word() }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Inflight {
    pub hash: B256,
    #[serde(flatten)]
    pub key: TxKey,
    pub nonce: u64,
    pub sent_block: u64,
    pub deadline_block: u64,
    pub gas_limit: u64,
    pub max_fee_wei: u128,
    pub priority_fee_wei: u128,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum Outcome {
    Included { block: u64, success: bool, gas_used: u64, effective_gas_price: u128, l1_fee_wei: u128 },
    /// No receipt well after the deadline (never included); the nonce is resynced.
    Dropped,
}

pub struct SendRequest {
    pub payload: Payload,
    pub min_profit: U256,
    pub gas_limit: u64,
    pub base_fee_wei: u128,
    pub priority_fee_wei: u128,
    pub key: TxKey,
    /// Refuse while another tx of `key.group` is in flight. Checked by the executor itself, so two
    /// callers racing for one group (each having checked `is_group_inflight` first) cannot both send.
    pub exclusive_group: bool,
    pub block: u64,
    pub deadline_block: u64,
}

/// A send the executor refused before signing anything: the in-flight rules or the cooldown, not a
/// failure (callers racing for one key or group meet these as a matter of course). Recognise it
/// with `err.downcast_ref::<SendRefused>()`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SendRefused {
    Halted { consecutive_failures: u32 },
    MaxInflight(usize),
    KeyInflight,
    GroupInflight,
}

impl std::fmt::Display for SendRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SendRefused::Halted { consecutive_failures } => write!(f, "executor halted after {consecutive_failures} consecutive failures"),
            SendRefused::MaxInflight(n) => write!(f, "{n} transaction(s) already in flight (max_inflight)"),
            SendRefused::KeyInflight => write!(f, "a transaction with this key is already in flight"),
            SendRefused::GroupInflight => write!(f, "a transaction of this group is already in flight"),
        }
    }
}

impl std::error::Error for SendRefused {}

/// When sending pauses: `max_in_window` failed (reverted) or dropped txs within `window` pause it
/// for `cooldown`, after which it resumes by itself. Losing a race is normal for both bots, so a
/// permanent halt (the old behaviour) would stop them exactly on the busy days that pay.
#[derive(Debug, Clone, Copy)]
pub struct FailurePolicy {
    pub max_in_window: u32,
    pub window: Duration,
    pub cooldown: Duration,
}

/// Failed / dropped txs within the policy window, and the pause a burst triggered.
#[derive(Debug)]
pub struct FailureWindow {
    policy: FailurePolicy,
    failures: VecDeque<Instant>,
    cool_until: Option<Instant>,
}

impl FailureWindow {
    pub fn new(policy: FailurePolicy) -> Self {
        Self { policy, failures: VecDeque::new(), cool_until: None }
    }

    pub fn cooling(&self, now: Instant) -> bool {
        self.cool_until.is_some_and(|t| now < t)
    }

    /// When the current cooldown ends (None = none started).
    pub fn cool_until(&self) -> Option<Instant> {
        self.cool_until
    }

    /// Records one failure at `now`; returns true when it started a cooldown.
    pub fn record(&mut self, now: Instant) -> bool {
        self.failures.push_back(now);
        while self.failures.front().is_some_and(|t| now.duration_since(*t) > self.policy.window) {
            self.failures.pop_front();
        }
        if self.failures.len() as u32 >= self.policy.max_in_window {
            self.failures.clear();
            self.cool_until = Some(now + self.policy.cooldown);
            return true;
        }
        false
    }
}

pub struct Executor<P> {
    provider: P,
    wallet: EthereumWallet,
    pub address: Address,
    chain_id: u64,
    bcast: Broadcaster,
    nonce: Option<u64>,
    /// The chain's pending nonce as last read by spawn_nonce_watch (0 = not running): the hot
    /// wallet may be shared with manual transfers or other senders, so the local nonce can fall behind.
    chain_nonce: Arc<AtomicU64>,
    /// Nonce coordination with the other process (None: this process signs alone).
    shared: Option<SharedNonce>,
    /// Sent, not yet settled, in nonce order. Several may be in flight at once (consecutive
    /// nonces): a crash makes many positions liquidatable in the same few blocks.
    pub inflight: Vec<Inflight>,
    /// At most this many in flight (1 = one at a time).
    max_inflight: usize,
    /// The hot wallet's balance as last read by spawn_nonce_watch (None = not read yet).
    balance: Arc<std::sync::Mutex<Option<U256>>>,
    pub consecutive_failures: u32,
    failures: FailureWindow,
}

/// A tx with no receipt this many blocks past its deadline is taken as dropped (it can only revert
/// with Expired if it still lands, and the nonce is resynced from the chain).
pub const DROP_GRACE_BLOCKS: u64 = 2;

/// How often spawn_nonce_watch reads the pending nonce.
pub const NONCE_WATCH_EVERY: Duration = Duration::from_secs(10);

impl<P: Provider + Clone + Send + Sync + 'static> Executor<P> {
    /// Reads the hot wallet's pending nonce and balance every `every` in the background, so that
    /// `send` never starts from a nonce another process (a manual transfer, another sender) already
    /// used, and the race path pays no RPC call for either.
    pub fn spawn_nonce_watch(&self, every: Duration) {
        let (p, address, slot, bal) = (self.provider.clone(), self.address, self.chain_nonce.clone(), self.balance.clone());
        tokio::spawn(async move {
            let mut t = tokio::time::interval(every);
            t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                t.tick().await;
                let (n, b) = tokio::join!(p.get_transaction_count(address).pending(), p.get_balance(address));
                if let Ok(n) = n {
                    // The latest reading, not a running max: a dropped tx lowers the pending nonce again.
                    slot.store(n, Ordering::Relaxed);
                }
                if let Ok(b) = b {
                    *bal.lock().unwrap() = Some(b);
                }
            }
        });
    }
}

impl<P: Provider> Executor<P> {
    pub fn new(provider: P, signer: PrivateKeySigner, chain_id: u64, bcast: Broadcaster, policy: FailurePolicy) -> Self {
        let address = signer.address();
        Self {
            provider,
            wallet: EthereumWallet::from(signer),
            address,
            chain_id,
            bcast,
            nonce: None,
            chain_nonce: Arc::default(),
            shared: None,
            inflight: Vec::new(),
            max_inflight: 1,
            balance: Arc::default(),
            consecutive_failures: 0,
            failures: FailureWindow::new(policy),
        }
    }

    /// Paused by a burst of failures (see FailurePolicy); resumes by itself after the cooldown.
    pub fn halted(&self) -> bool {
        self.failures.cooling(Instant::now())
    }

    /// When the failure cooldown ends (None = never started).
    pub fn cool_until(&self) -> Option<Instant> {
        self.failures.cool_until()
    }

    /// The background balance reading (shared with spawn_nonce_watch).
    pub fn balance_cell(&self) -> Arc<std::sync::Mutex<Option<U256>>> {
        self.balance.clone()
    }

    /// Up to `n` txs in flight at once (consecutive nonces).
    pub fn with_max_inflight(mut self, n: usize) -> Self {
        self.max_inflight = n.max(1);
        self
    }

    /// Not halted and below max_inflight.
    pub fn can_send(&self) -> bool {
        !self.halted() && self.inflight.len() < self.max_inflight
    }

    /// A tx with this key is already in flight.
    pub fn is_inflight(&self, key: TxKey) -> bool {
        self.inflight.iter().any(|i| i.key == key)
    }

    /// A tx in this group is in flight.
    pub fn is_group_inflight(&self, group: u64) -> bool {
        self.inflight.iter().any(|i| i.key.group == group)
    }

    /// The configured maximum in flight.
    pub fn max_inflight(&self) -> usize {
        self.max_inflight
    }

    /// The last background balance reading minus what the txs in flight may still burn at most
    /// (None = no reading yet: call `balance`).
    pub fn spendable(&self) -> Option<U256> {
        let b = (*self.balance.lock().unwrap())?;
        let reserved: u128 = self.inflight.iter().map(|i| i.gas_limit as u128 * i.max_fee_wei).sum();
        Some(b.saturating_sub(U256::from(reserved)))
    }

    /// Records a failed / dropped tx (may start a cooldown).
    fn record_failure(&mut self) {
        self.consecutive_failures += 1;
        if self.failures.record(Instant::now()) {
            let p = self.failures.policy;
            warn!(window_s = p.window.as_secs(), cooldown_s = p.cooldown.as_secs(), "too many failed txs: sending paused, resumes by itself");
        }
    }

    pub async fn sync_nonce(&mut self) -> Result<u64> {
        let n = self.provider.get_transaction_count(self.address).pending().await?;
        self.nonce = Some(n);
        // Fresher than the watcher's reading, which can still count a tx that was since dropped.
        self.chain_nonce.store(n, Ordering::Relaxed);
        Ok(n)
    }

    /// Coordinates nonces with the other process signing for the same hot wallet (SharedNonce).
    pub fn with_shared_nonce(mut self, path: impl Into<std::path::PathBuf>) -> Self {
        let path = path.into();
        // The default (ExecConfig::nonce_path) is under the user state dir, which may not exist yet;
        // a failure here surfaces as a clear "opening <path>" error on the first send.
        if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
            let _ = std::fs::create_dir_all(dir);
        }
        self.shared = Some(SharedNonce { path });
        self
    }

    pub async fn balance(&self) -> Result<U256> {
        Ok(self.provider.get_balance(self.address).await?)
    }

    /// Signs and broadcasts one tx. Refuses (`SendRefused`, nothing signed) when halted, at
    /// max_inflight, when a tx with the same key is already in flight, or (`exclusive_group`) one
    /// of the same group.
    pub async fn send(&mut self, r: SendRequest) -> Result<Inflight> {
        if self.halted() {
            return Err(SendRefused::Halted { consecutive_failures: self.consecutive_failures }.into());
        }
        if self.inflight.len() >= self.max_inflight {
            return Err(SendRefused::MaxInflight(self.inflight.len()).into());
        }
        if self.is_inflight(r.key) {
            return Err(SendRefused::KeyInflight.into());
        }
        if r.exclusive_group && self.is_group_inflight(r.key.group) {
            return Err(SendRefused::GroupInflight.into());
        }
        // Held from here until the broadcast is done: the other process waits for it.
        let mut lock = match &self.shared {
            Some(s) => match s.lock().await {
                Ok(f) => Some(f),
                // Fail closed: without the lock another bot on this wallet may sign with the same
                // nonce, and one of the two txs is lost.
                Err(e) => bail!("shared nonce file unavailable, not sending: {e}"),
            },
            None => None,
        };
        let shared_next = match lock.as_mut().map(SharedNonce::read) {
            None | Some(SharedEntry::None) => 0,
            Some(SharedEntry::Next(n)) => n,
            // Another bot on this wallet writes a format this version does not read: its nonce is
            // unknown, so sending could collide with it.
            Some(SharedEntry::Unknown) => bail!("shared nonce file has an unknown format (a bot built on another onchain-core version?); not sending"),
        };
        // The local nonce, unless the chain or the other process is already past it (a send by
        // `watch` or a manual transfer).
        let mut nonce = match self.nonce {
            Some(n) => n.max(self.chain_nonce.load(Ordering::Relaxed)),
            None => self.sync_nonce().await?,
        }
        .max(shared_next);
        let calldata = patch_min_profit(&r.payload.calldata, r.payload.min_profit, r.min_profit)?;
        // Room for the base fee to double before inclusion; the priority fee is what we bid.
        let max_fee = r.base_fee_wei * 2 + r.priority_fee_wei;
        let mut sent = self.sign_and_send(&r, &calldata, nonce, max_fee).await;
        // Still stale (it moved within the last watch interval): resync and resend once, now,
        // rather than lose the opportunity.
        if let Err(e) = &sent
            && nonce_conflict(&e.to_string())
        {
            let fresh = match self.sync_nonce().await {
                Ok(f) => f,
                Err(err) => {
                    self.nonce = None; // do not start the next send from the stale one
                    return Err(err.context("resync after a nonce conflict"));
                }
            };
            if fresh > nonce {
                warn!(stale = nonce, fresh, "nonce was stale (hot wallet used elsewhere); resending");
                nonce = fresh;
                sent = self.sign_and_send(&r, &calldata, nonce, max_fee).await;
            }
        }
        match sent {
            Ok(hash) => {
                self.nonce = Some(nonce + 1);
                if let Some(f) = lock.as_mut() {
                    SharedNonce::write(f, nonce + 1);
                }
                let inflight = Inflight {
                    hash,
                    key: r.key,
                    nonce,
                    sent_block: r.block,
                    deadline_block: r.deadline_block,
                    gas_limit: r.gas_limit,
                    max_fee_wei: max_fee,
                    priority_fee_wei: r.priority_fee_wei,
                };
                self.inflight.push(inflight.clone());
                Ok(inflight)
            }
            Err(e) => {
                // Nonce may be stale (e.g. "nonce too low"): resync before the next attempt.
                self.nonce = None;
                Err(e)
            }
        }
    }

    async fn sign_and_send(&self, r: &SendRequest, calldata: &Bytes, nonce: u64, max_fee: u128) -> Result<B256> {
        let tx = TransactionRequest::default()
            .with_from(self.address)
            .with_to(r.payload.to)
            .with_input(calldata.clone())
            .with_nonce(nonce)
            .with_chain_id(self.chain_id)
            .with_gas_limit(r.gas_limit)
            .with_max_fee_per_gas(max_fee)
            .with_max_priority_fee_per_gas(r.priority_fee_wei);
        let envelope = <TransactionRequest as NetworkTransactionBuilder<Ethereum>>::build(tx, &self.wallet).await.context("signing")?;
        let hash = *envelope.tx_hash();
        self.bcast.send_raw(&envelope.encoded_2718(), hash).await
    }

    /// Checks every tx in flight (receipts fetched in parallel). Returns the outcomes now known and
    /// clears those txs. A tx with no receipt DROP_GRACE_BLOCKS past its deadline is dropped, and so
    /// is every later-nonce tx still without a receipt: none of them can land before it.
    pub async fn poll(&mut self, current_block: u64) -> Result<Vec<(Inflight, Outcome)>> {
        if self.inflight.is_empty() {
            return Ok(vec![]);
        }
        let receipts = futures::future::join_all(self.inflight.iter().map(|i| self.provider.raw_request::<_, Value>("eth_getTransactionReceipt".into(), (i.hash,)))).await;
        let hex = |v: &Value| v.as_str().and_then(|s| u128::from_str_radix(s.trim_start_matches("0x"), 16).ok()).unwrap_or(0);
        let mut out = vec![];
        let mut keep = vec![];
        let mut dropped_from: Option<u64> = None;
        let mut first_err = None;
        for (inf, r) in std::mem::take(&mut self.inflight).into_iter().zip(receipts) {
            match r {
                Ok(receipt) if !receipt.is_null() => {
                    let success = receipt["status"].as_str() == Some("0x1");
                    let outcome = Outcome::Included {
                        block: hex(&receipt["blockNumber"]) as u64,
                        success,
                        gas_used: hex(&receipt["gasUsed"]) as u64,
                        effective_gas_price: hex(&receipt["effectiveGasPrice"]),
                        l1_fee_wei: hex(&receipt["l1Fee"]),
                    };
                    if success {
                        self.consecutive_failures = 0;
                    } else {
                        self.record_failure();
                    }
                    out.push((inf, outcome));
                }
                Ok(_) if dropped_from.is_some_and(|n| inf.nonce > n) || current_block > inf.deadline_block + DROP_GRACE_BLOCKS => {
                    dropped_from = Some(dropped_from.map_or(inf.nonce, |n| n.min(inf.nonce)));
                    self.record_failure();
                    out.push((inf, Outcome::Dropped));
                }
                Ok(_) => keep.push(inf),
                Err(e) => {
                    first_err.get_or_insert(e);
                    keep.push(inf);
                }
            }
        }
        // Later nonces kept only while nothing before them was dropped.
        if let Some(n) = dropped_from {
            for inf in std::mem::take(&mut keep) {
                if inf.nonce > n {
                    self.record_failure();
                    out.push((inf, Outcome::Dropped));
                } else {
                    keep.push(inf);
                }
            }
        }
        self.inflight = keep;
        if dropped_from.is_some() {
            let dropped: Vec<Inflight> = out.iter().filter(|(_, o)| matches!(o, Outcome::Dropped)).map(|(i, _)| i.clone()).collect();
            self.cancel_stuck(&dropped).await;
            // The outcomes above are already out of `inflight`: a failed resync must not lose them.
            // The next send resyncs instead.
            if let Err(e) = self.sync_nonce().await {
                warn!(error = %e, "nonce resync after a dropped tx failed; the next send resyncs");
                self.nonce = None;
            }
        }
        // A settled tx is no longer reserved by `spendable`: read the balance it left now, not at
        // the next background reading.
        if !out.is_empty()
            && let Ok(b) = self.provider.get_balance(self.address).await
        {
            *self.balance.lock().unwrap() = Some(b);
        }
        if let (Some(e), true) = (first_err, out.is_empty()) {
            return Err(e.into());
        }
        Ok(out)
    }
}

impl<P: Provider> Executor<P> {
    /// Txs given up on (no receipt past their deadline) may still sit in the mempool, e.g. a max fee
    /// the base fee rose above, and then every later nonce waits behind them. Each one still
    /// pending is replaced by a 0-value self-transfer with the same nonce at a fee that can land
    /// (replacement needs both fees >= 12.5% higher); it would only revert (Expired) anyway.
    async fn cancel_stuck(&self, dropped: &[Inflight]) {
        let (latest, pending) = tokio::join!(self.provider.get_transaction_count(self.address), self.provider.get_transaction_count(self.address).pending());
        let (Ok(latest), Ok(pending)) = (latest, pending) else { return };
        let base_fee = match self.provider.get_block_by_number(alloy::eips::BlockNumberOrTag::Latest).await {
            Ok(Some(b)) => b.header.base_fee_per_gas.unwrap_or(0) as u128,
            _ => return,
        };
        let bump = |x: u128| x + x / 8 + 1;
        for inf in dropped.iter().filter(|i| i.nonce >= latest && i.nonce < pending) {
            let priority = bump(inf.priority_fee_wei);
            let max_fee = bump(inf.max_fee_wei).max(base_fee * 2 + priority);
            let tx = TransactionRequest::default()
                .with_from(self.address)
                .with_to(self.address)
                .with_value(U256::ZERO)
                .with_nonce(inf.nonce)
                .with_chain_id(self.chain_id)
                .with_gas_limit(21_000)
                .with_max_fee_per_gas(max_fee)
                .with_max_priority_fee_per_gas(priority);
            let sent = async {
                let envelope = <TransactionRequest as NetworkTransactionBuilder<Ethereum>>::build(tx, &self.wallet).await.context("signing")?;
                self.bcast.send_raw(&envelope.encoded_2718(), *envelope.tx_hash()).await
            };
            match sent.await {
                Ok(hash) => warn!(stuck = %inf.hash, nonce = inf.nonce, cancel = %hash, max_fee_gwei = max_fee as f64 / 1e9, "stuck tx replaced by a 0-value self-transfer"),
                Err(e) => warn!(stuck = %inf.hash, nonce = inf.nonce, error = %e, "could not replace a stuck tx"),
            }
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nonce_errors_are_recognised_across_endpoints() {
        assert!(nonce_conflict("nonce too low: next nonce 40, tx nonce 39"));
        assert!(nonce_conflict("nonce=39 minNonce=40 txHash=0x7e2e: nonce too low"));
        assert!(nonce_conflict("no endpoint accepted the tx: Nonce too low | transport error"));
        assert!(!nonce_conflict("insufficient funds for gas * price + value"));
        assert!(nonce_conflict("replacement transaction underpriced"), "a pending tx of the other process holds it");
    }

    #[tokio::test]
    async fn shared_nonce_is_locked_recorded_and_expires() {
        let path = std::env::temp_dir().join(format!("onchain-core-nonce-test-{}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let s = SharedNonce { path: path.clone() };
        let mut f = s.lock().await.unwrap();
        assert_eq!(SharedNonce::read(&mut f), SharedEntry::None, "empty file: nothing recorded");
        // Held: a second taker (the other process) must wait, then give up after SHARED_NONCE_WAIT.
        let other = std::fs::OpenOptions::new().read(true).write(true).open(&path).unwrap();
        assert!(matches!(other.try_lock(), Err(std::fs::TryLockError::WouldBlock)));
        SharedNonce::write(&mut f, 42);
        assert_eq!(SharedNonce::read(&mut f), SharedEntry::Next(42));
        drop(f);
        let mut f = s.lock().await.unwrap();
        assert_eq!(SharedNonce::read(&mut f), SharedEntry::Next(42), "kept across locks");
        // An old entry (its tx may have been dropped) is ignored.
        std::fs::write(&path, format!("v1 43 {}", unix_ms() - SHARED_NONCE_TTL.as_millis() as u64 - 1000)).unwrap();
        assert_eq!(SharedNonce::read(&mut f), SharedEntry::None);
        drop(f);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn shared_nonce_format_is_versioned_and_unknown_formats_are_not_trusted() {
        let now = 1_000_000_000u64;
        assert_eq!(SharedNonce::parse("v1 7 999999000", now), SharedEntry::Next(7));
        assert_eq!(SharedNonce::parse("", now), SharedEntry::None);
        assert_eq!(SharedNonce::parse("v1 7 1", now), SharedEntry::None, "stale");
        // A fresh entry in the pre-versioned format and anything else are unknown, never read as a
        // nonce.
        for s in ["7 999999000", "v2 7 999999000", "v1 x 999999000", "v1 7 999999000 extra", "x 1"] {
            assert_eq!(SharedNonce::parse(s, now), SharedEntry::Unknown, "{s}");
        }
        // A stale one, left by an earlier release, does not block sending after an upgrade.
        assert_eq!(SharedNonce::parse("7 1", now), SharedEntry::None);
    }

    /// Two processes (this test binary started twice) take turns on one shared nonce file: each
    /// child locks it, reads the next nonce, writes next + 1, many times. If the lock did not
    /// exclude the other process, two children would read the same value and the final count
    /// would come up short.
    #[test]
    fn shared_nonce_lock_serialises_two_processes() {
        let path = std::env::temp_dir().join(format!("onchain-core-nonce-xproc-{}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let exe = std::env::current_exe().unwrap();
        let spawn = || {
            std::process::Command::new(&exe)
                .args(["--exact", "executor::tests::child_bumps_the_shared_nonce", "--ignored", "--nocapture", "--test-threads=1"])
                .env("ONCHAIN_CORE_NONCE_PATH", &path)
                .spawn()
                .unwrap()
        };
        let (mut a, mut b) = (spawn(), spawn());
        assert!(a.wait().unwrap().success() && b.wait().unwrap().success());
        let s = std::fs::read_to_string(&path).unwrap();
        assert_eq!(SharedNonce::parse(&s, unix_ms()), SharedEntry::Next(2 * CHILD_BUMPS), "{s}");
        let _ = std::fs::remove_file(&path);
    }

    const CHILD_BUMPS: u64 = 200;

    #[tokio::test]
    #[ignore = "child half of shared_nonce_lock_serialises_two_processes"]
    async fn child_bumps_the_shared_nonce() {
        // Only meaningful when started by its parent test (`cargo test -- --ignored` runs it alone).
        let Ok(path) = std::env::var("ONCHAIN_CORE_NONCE_PATH") else { return };
        let s = SharedNonce { path: path.into() };
        for _ in 0..CHILD_BUMPS {
            let mut f = s.lock().await.unwrap();
            let next = match SharedNonce::read(&mut f) {
                SharedEntry::Next(n) => n,
                SharedEntry::None => 0,
                SharedEntry::Unknown => panic!("unknown format"),
            };
            SharedNonce::write(&mut f, next + 1);
        }
    }

    #[test]
    fn a_burst_of_failures_pauses_then_resumes() {
        let s = Duration::from_secs;
        let mut w = FailureWindow::new(FailurePolicy { max_in_window: 3, window: s(600), cooldown: s(1800) });
        let t0 = Instant::now();
        assert!(!w.record(t0));
        assert!(!w.record(t0 + s(100)));
        assert!(!w.cooling(t0 + s(100)));
        assert!(w.record(t0 + s(200)), "third failure within 10 min starts the pause");
        assert!(w.cooling(t0 + s(1000)));
        assert!(!w.cooling(t0 + s(200) + s(1801)), "resumes by itself after the cooldown");
    }

    #[test]
    fn failures_spread_out_never_pause() {
        let s = Duration::from_secs;
        let mut w = FailureWindow::new(FailurePolicy { max_in_window: 3, window: s(600), cooldown: s(1800) });
        let t0 = Instant::now();
        for i in 0..10u64 {
            assert!(!w.record(t0 + s(700 * i)), "one failure every ~12 min is just racing");
        }
    }

    #[test]
    fn min_profit_patch_packed_and_abi() {
        // A packed uint128 field and an ABI word, as contracts encode minProfit.
        const PACKED_MIN_PROFIT: MinProfitSlot = MinProfitSlot { offset: 125, width: 16 };
        const ABI_LIQUIDATE_MIN_PROFIT: MinProfitSlot = MinProfitSlot { offset: 4 + 9 * 32, width: 32 };
        let cd: Bytes = vec![0u8; 200].into();
        let p = patch_min_profit(&cd, PACKED_MIN_PROFIT, U256::from(0x0102u64)).unwrap();
        assert_eq!(&p[125..139], &[0u8; 14]);
        assert_eq!(&p[139..141], &[1, 2]);
        assert!(p[..125].iter().chain(&p[141..]).all(|b| *b == 0), "only the field changes");
        let cd: Bytes = vec![0u8; 4 + 11 * 32].into();
        let p = patch_min_profit(&cd, ABI_LIQUIDATE_MIN_PROFIT, U256::from(7u64)).unwrap();
        assert_eq!(p[4 + 9 * 32 + 31], 7);
        // A value wider than the packed field is refused rather than truncated.
        assert!(patch_min_profit(&vec![0u8; 200].into(), PACKED_MIN_PROFIT, U256::MAX).is_err());
    }


    fn temp_keystore(tag: &str) -> (std::path::PathBuf, PrivateKeySigner) {
        let dir = std::env::temp_dir().join(format!("bot-ks-{}-{tag}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let (signer, _) = PrivateKeySigner::new_keystore(&dir, &mut rand::thread_rng(), "test-pw-not-secret", Some("ks.json")).unwrap();
        (dir, signer)
    }

    #[test]
    fn keystore_decrypt_roundtrip_and_no_password_echo() {
        let (dir, signer) = temp_keystore("rt");
        let ks = dir.join("ks.json");
        assert_eq!(decrypt_signer(ks.to_str().unwrap(), "test-pw-not-secret").unwrap().address(), signer.address());
        let err = decrypt_signer(ks.to_str().unwrap(), "pw-XYZZY-123").unwrap_err().to_string();
        assert!(!err.contains("XYZZY"), "error must not echo the password");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Runs in a child process (single test, single thread) because removing an env var is only
    /// sound when no other thread exists: exactly the situation main() guarantees.
    #[test]
    fn password_env_is_read_then_removed_in_a_single_threaded_process() {
        let (dir, signer) = temp_keystore("child");
        let exe = std::env::current_exe().unwrap();
        let out = std::process::Command::new(exe)
            .args(["--exact", "executor::tests::child_load_signer_from_env", "--ignored", "--nocapture", "--test-threads=1"])
            .env("BOT_TEST_KS_PASSWORD", "test-pw-not-secret")
            .env("BOT_TEST_KS_PATH", dir.join("ks.json"))
            .env("BOT_TEST_KS_ADDR", signer.address().to_string())
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert!(stdout.contains("CHILD_OK"), "child failed:\n{stdout}\n{}", String::from_utf8_lossy(&out.stderr));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    #[ignore = "child half of password_env_is_read_then_removed_in_a_single_threaded_process"]
    fn child_load_signer_from_env() {
        // Only meaningful when started by its parent test (`cargo test -- --ignored` runs it alone).
        let Ok(path) = std::env::var("BOT_TEST_KS_PATH") else { return };
        let s = load_signer(&path, "BOT_TEST_KS_PASSWORD").unwrap();
        assert_eq!(s.address().to_string(), std::env::var("BOT_TEST_KS_ADDR").unwrap());
        assert!(std::env::var("BOT_TEST_KS_PASSWORD").is_err(), "password env var must be removed after use");
        println!("CHILD_OK");
    }
}
