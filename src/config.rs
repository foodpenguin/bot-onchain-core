//! Config pieces every bot shares, meant to be `#[serde(flatten)]`ed into a bot's own config:
//! `RpcSettings` (top level: endpoints and the `.env` that holds their keys) and `SendSettings`
//! (inside the bot's `[exec]`: wallet, keystore, broadcast, gates, failure policy).

use std::{collections::HashMap, path::PathBuf, time::Duration};

use alloy::primitives::Address;
use anyhow::{Context, Result};
use serde::Deserialize;

use crate::{executor::FailurePolicy, rpc::EndpointSpec};

#[derive(Debug, Deserialize, Clone)]
pub struct RpcEntry {
    pub name: String,
    /// URL written in the config (public endpoints only).
    #[serde(default)]
    pub http: Option<String>,
    /// Name of the env var (in `env_file` or the process env) holding the URL with its key.
    #[serde(default)]
    pub http_env: Option<String>,
    /// Lower = preferred. Equal priorities are tie-broken by measured latency.
    #[serde(default)]
    pub priority: u32,
    /// newHeads source: "derive" (from the HTTP URL), a wss:// URL, or absent for none.
    #[serde(default)]
    pub ws: Option<String>,
    /// JSON-RPC methods never routed here (unsupported or too slow on this endpoint), e.g.
    /// ["eth_simulateV1"]. Methods answered with "method not found" are also learned at runtime.
    #[serde(default)]
    pub skip_methods: Vec<String>,
    /// Largest eth_getLogs block range this endpoint accepts (e.g. 10 on Alchemy's free plan);
    /// wider queries go to other endpoints instead of being refused here.
    #[serde(default)]
    pub max_log_blocks: Option<u64>,
}

/// Top-level RPC keys.
#[derive(Debug, Deserialize, Clone, Default)]
pub struct RpcSettings {
    /// Single endpoint (simple setup). Ignored when `[[rpc]]` entries are present.
    #[serde(default)]
    pub rpc_http: Option<String>,
    /// WebSocket endpoint for `eth_subscribe newHeads` with `rpc_http`; `[[rpc]].ws` supersedes it.
    #[serde(default)]
    pub rpc_ws: Option<String>,
    /// .env file holding endpoint URLs with API keys (referenced by `http_env`).
    #[serde(default)]
    pub env_file: Option<String>,
    /// Load-balanced endpoints (see rpc.rs).
    #[serde(default)]
    pub rpc: Vec<RpcEntry>,
}

/// Resolved endpoints. URLs contain secrets: keep them out of logs.
pub struct ResolvedRpc {
    pub http: Vec<EndpointSpec>,
    /// (endpoint name, wss url)
    pub ws: Vec<(String, String)>,
}

impl RpcSettings {
    /// A relative `env_file` is read relative to the config file's directory.
    pub fn anchor(&mut self, config_path: &str) {
        if let Some(e) = &self.env_file {
            self.env_file = Some(anchored(config_path, e));
        }
    }

    pub fn resolve(&self) -> Result<ResolvedRpc> {
        let env = match &self.env_file {
            Some(p) if std::path::Path::new(p).exists() => crate::rpc::load_env_file(p)?,
            Some(p) => {
                tracing::warn!(env_file = %p, "env file not found; endpoints that need it are skipped");
                HashMap::new()
            }
            None => HashMap::new(),
        };
        let mut out = ResolvedRpc { http: Vec::new(), ws: Vec::new() };
        if self.rpc.is_empty() {
            let url = self.rpc_http.clone().context("config needs [[rpc]] entries or rpc_http")?;
            out.http.push(EndpointSpec { name: "default".into(), url, priority: 0, skip_methods: vec![], max_log_blocks: None });
            if let Some(ws) = &self.rpc_ws {
                out.ws.push(("default".into(), ws.clone()));
            }
            return Ok(out);
        }
        for e in &self.rpc {
            let url = match (&e.http, &e.http_env) {
                (Some(u), _) => u.clone(),
                (None, Some(var)) => match env.get(var).cloned().or_else(|| std::env::var(var).ok()) {
                    Some(u) => u,
                    None => {
                        // Missing key (e.g. .env not copied yet on a new machine): run on the
                        // remaining endpoints instead of refusing to start.
                        tracing::warn!(rpc = %e.name, var = %var, "env var not found; endpoint skipped");
                        continue;
                    }
                },
                (None, None) => anyhow::bail!("rpc {}: needs http or http_env", e.name),
            };
            match e.ws.as_deref() {
                Some("derive") => {
                    let ws = crate::rpc::ws_from_http(&url).with_context(|| format!("rpc {}: cannot derive ws url", e.name))?;
                    out.ws.push((e.name.clone(), ws));
                }
                Some(u) => out.ws.push((e.name.clone(), u.to_string())),
                None => {}
            }
            out.http.push(EndpointSpec { name: e.name.clone(), url, priority: e.priority, skip_methods: e.skip_methods.clone(), max_log_blocks: e.max_log_blocks });
        }
        anyhow::ensure!(!out.http.is_empty(), "no usable RPC endpoint (check .env / [[rpc]])");
        Ok(out)
    }
}

/// The `[exec]` keys every bot shares.
#[derive(Debug, Deserialize, Clone)]
pub struct SendSettings {
    /// Hot wallet address (only the address; the key never goes in config).
    pub executor: Address,
    /// "shadow" (default: never sends) or "live". Live also needs the bot's own command-line
    /// switch: two switches, so neither a stray flag nor a copied config sends alone.
    #[serde(default = "default_mode")]
    pub mode: String,
    /// Encrypted keystore of the hot wallet (`cast wallet import`). Never a raw key.
    #[serde(default)]
    pub keystore: Option<String>,
    /// Env var holding the keystore password (removed right after reading); else a prompt.
    #[serde(default = "default_pw_env")]
    pub keystore_password_env: String,
    /// Extra endpoints the raw tx is broadcast to, besides every [[rpc]] endpoint. Opt-in (default
    /// empty): a config that forgets it must not send a fork's test txs to mainnet.
    #[serde(default)]
    pub extra_broadcast: Vec<String>,
    /// ETH/USD used for gas costs until an on-chain price is read.
    pub eth_usd_fallback: f64,
    /// If this file exists, nothing is sent. Relative = next to the config file (see `anchor`).
    #[serde(default = "default_kill_file")]
    pub kill_switch_file: String,
    /// File every process signing for the hot wallet locks around sign + broadcast, recording the
    /// next nonce (executor::SharedNonce). Empty (default) = one file per wallet under the user's
    /// state dir (see `nonce_path`), so bots in different repos/directories still share it.
    #[serde(default)]
    pub nonce_file: String,
    /// Gas burned by FAILED (reverted) txs per UTC day, in USD; once spent, no more sends that day.
    /// Successful sends pay for their own gas and do not count.
    #[serde(default = "default_daily_budget")]
    pub daily_gas_budget_usd: f64,
    /// Sending pauses for failure_cooldown_secs when this many failed (reverted) or dropped txs
    /// fall within failure_window_secs, then resumes by itself (no permanent halt: losing races is
    /// normal on the busy days that pay).
    #[serde(default = "default_max_fail")]
    pub max_failures_in_window: u32,
    #[serde(default = "default_fail_window")]
    pub failure_window_secs: u64,
    #[serde(default = "default_fail_cooldown")]
    pub failure_cooldown_secs: u64,
    /// Txs in flight at once (consecutive nonces). 1 (default) = one at a time.
    #[serde(default = "default_max_inflight")]
    pub max_inflight: usize,
    /// Refuse live mode below this hot-wallet balance (ETH).
    #[serde(default = "default_min_balance")]
    pub min_wallet_eth: f64,
}

impl SendSettings {
    /// Paths relative to the config file's directory (see `anchored`).
    pub fn anchor(&mut self, config_path: &str) {
        self.kill_switch_file = anchored(config_path, &self.kill_switch_file);
    }

    pub fn is_live(&self) -> bool {
        self.mode == "live"
    }

    pub fn failure_policy(&self) -> FailurePolicy {
        FailurePolicy {
            max_in_window: self.max_failures_in_window,
            window: Duration::from_secs(self.failure_window_secs),
            cooldown: Duration::from_secs(self.failure_cooldown_secs),
        }
    }

    pub fn validate(&self) -> Result<()> {
        anyhow::ensure!(["shadow", "live"].contains(&self.mode.as_str()), "exec.mode = {:?}: must be \"shadow\" or \"live\"", self.mode);
        anyhow::ensure!(self.max_inflight >= 1, "exec.max_inflight must be at least 1");
        Ok(())
    }

    /// Where the shared nonce file for `wallet` lives. Default: `$XDG_STATE_HOME` (or
    /// `~/.local/state`) `/onchain-bots/nonce-<wallet>`, the same for every bot using this wallet
    /// whatever its working directory. An explicit `nonce_file` is used as given; a relative one only
    /// coordinates with processes started from the same directory (warned at startup).
    pub fn nonce_path(&self, wallet: Address) -> PathBuf {
        if !self.nonce_file.is_empty() {
            let p = PathBuf::from(&self.nonce_file);
            if p.is_relative() {
                tracing::warn!(nonce_file = %self.nonce_file, "relative exec.nonce_file: only bots started from this directory share it; leave it empty to share per wallet");
            }
            return p;
        }
        let state = std::env::var_os("XDG_STATE_HOME")
            .map(PathBuf::from)
            .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".local").join("state")))
            .unwrap_or_else(|| PathBuf::from("data"));
        state.join("onchain-bots").join(format!("nonce-{}", wallet.to_string().to_lowercase()))
    }
}

/// A relative path from the config, anchored at the config file's directory: the same file whatever
/// directory the bot is started from (a kill switch that silently points elsewhere does not stop it).
pub fn anchored(config_path: &str, p: &str) -> String {
    let path = std::path::Path::new(p);
    if p.is_empty() || path.is_absolute() {
        return p.to_string();
    }
    let dir = std::path::Path::new(config_path).parent().filter(|d| !d.as_os_str().is_empty()).unwrap_or(std::path::Path::new("."));
    dir.join(path).to_string_lossy().into_owned()
}

fn default_mode() -> String {
    "shadow".into()
}
fn default_pw_env() -> String {
    "BOT_KEYSTORE_PASSWORD".into()
}
fn default_kill_file() -> String {
    "data/KILL".into()
}
fn default_daily_budget() -> f64 {
    2.0
}
fn default_max_fail() -> u32 {
    3
}
fn default_fail_window() -> u64 {
    600
}
fn default_fail_cooldown() -> u64 {
    1800
}
fn default_max_inflight() -> usize {
    1
}
fn default_min_balance() -> f64 {
    0.0005
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Deserialize)]
    struct BotExec {
        #[serde(flatten)]
        send: SendSettings,
        own_key: u32,
    }
    #[derive(Deserialize)]
    struct BotConfig {
        #[serde(flatten)]
        rpc: RpcSettings,
        exec: BotExec,
    }

    #[test]
    fn shared_sections_flatten_into_a_bots_config() {
        let t = r#"
            env_file = "../.env"
            [[rpc]]
            name = "public"
            http = "https://example.invalid"
            [exec]
            executor = "0x000000000000000000000000000000000000dEaD"
            eth_usd_fallback = 2400.0
            own_key = 7
        "#;
        let mut c: BotConfig = toml::from_str(t).unwrap();
        c.exec.send.anchor("bot/config.toml");
        c.rpc.anchor("bot/config.toml");
        assert_eq!(c.exec.own_key, 7);
        assert_eq!((c.exec.send.mode.as_str(), c.exec.send.max_inflight), ("shadow", 1), "safe defaults");
        assert_eq!(c.exec.send.kill_switch_file, "bot/data/KILL");
        assert_eq!(c.rpc.env_file.as_deref(), Some("bot/../.env"));
        assert_eq!(c.rpc.resolve().unwrap().http[0].name, "public");
        c.exec.send.validate().unwrap();
        c.exec.send.mode = "lvie".into();
        assert!(c.exec.send.validate().is_err(), "a typo never means live");
    }
}
