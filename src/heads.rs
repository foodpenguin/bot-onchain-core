//! New-block source. One background task per WebSocket endpoint keeps an
//! `eth_subscribe newHeads` subscription alive (reconnecting on drop); all of them feed one
//! channel, so whichever endpoint sees a block first wins. Without WS (or if every socket goes
//! quiet) the main loop polls over HTTP.

use std::time::{Duration, Instant};

use alloy::{
    eips::BlockNumberOrTag,
    providers::{Provider, ProviderBuilder, WsConnect},
};
use anyhow::{Context, Result};
use futures::StreamExt;
use tokio::sync::watch;
use tracing::{info, warn};

/// No head for this long = the socket is dead, whatever it claims (3 blocks at 2 s; ~30 at 200 ms).
const WS_STALL: Duration = Duration::from_secs(6);

/// WebSocket reconnect backoff: exponential from `MIN` to `MAX`. An endpoint that fails more than
/// `MAX_RECONNECTS_PER_HOUR` is parked for `PARK` (one WARN) so a dead free-tier socket cannot spin
/// a reconnect/error storm.
const RECONNECT_MIN: Duration = Duration::from_secs(1);
const RECONNECT_MAX: Duration = Duration::from_secs(60);
const RECONNECT_PARK: Duration = Duration::from_secs(300);
const MAX_RECONNECTS_PER_HOUR: usize = 20;

/// Source index used for heads that came from HTTP polling.
pub const SOURCE_HTTP: usize = usize::MAX;
/// Head fetched over HTTP when the flashblock stream signalled the block sealed.
pub const SOURCE_FLASHBLOCK: usize = usize::MAX - 1;

#[derive(Clone, Copy, Debug)]
pub struct Head {
    pub number: u64,
    pub timestamp: u64,
    pub base_fee: u128,
    /// When this process learned about the block (for latency accounting).
    pub seen_at: Instant,
    /// Index of the WS endpoint that delivered it first (SOURCE_HTTP for polling).
    pub source: usize,
}

pub async fn http_head<P: Provider>(p: &P, tag: BlockNumberOrTag) -> Result<Head> {
    let b = p.get_block_by_number(tag).await?.context("block not found")?;
    Ok(Head {
        number: b.header.number,
        timestamp: b.header.timestamp,
        base_fee: b.header.base_fee_per_gas.unwrap_or(0) as u128,
        seen_at: Instant::now(),
        source: SOURCE_HTTP,
    })
}

/// Spawns one subscriber per (name, url); returns a receiver that always holds the newest head.
pub fn spawn_ws_many(endpoints: Vec<(String, String)>) -> watch::Receiver<Option<Head>> {
    let (tx, rx) = watch::channel(None);
    for (source, (name, url)) in endpoints.into_iter().enumerate() {
        let tx = tx.clone();
        tokio::spawn(async move {
            let mut backoff = RECONNECT_MIN;
            // Failures in the last hour: an endpoint that flaps constantly (e.g. drpc's free WS after
            // Denim) is parked for RECONNECT_PARK so it stops hammering reconnects (and alloy stops
            // logging an error per attempt), with one WARN instead of thousands.
            let mut fails: std::collections::VecDeque<Instant> = std::collections::VecDeque::new();
            loop {
                let started = Instant::now();
                match subscribe(&name, &url, source, &tx).await {
                    Ok(()) => warn!(endpoint = %name, "ws head stream ended; reconnecting"),
                    // The error text can embed the URL (and its key), so only the name is logged.
                    Err(e) if e.to_string().starts_with("no head for") => warn!(endpoint = %name, error = %e, "ws head stream stalled; reconnecting"),
                    Err(_) => warn!(endpoint = %name, "ws head subscription failed; reconnecting"),
                }
                if tx.is_closed() {
                    return;
                }
                // A connection that lived a while was healthy: reset the backoff and the window.
                if started.elapsed() > Duration::from_secs(60) {
                    backoff = RECONNECT_MIN;
                    fails.clear();
                }
                let now = Instant::now();
                fails.push_back(now);
                while fails.front().is_some_and(|t| now.duration_since(*t) > Duration::from_secs(3600)) {
                    fails.pop_front();
                }
                let wait = if fails.len() > MAX_RECONNECTS_PER_HOUR {
                    warn!(endpoint = %name, fails = fails.len(), park_s = RECONNECT_PARK.as_secs(), "ws endpoint failing repeatedly; parking it");
                    fails.clear();
                    RECONNECT_PARK
                } else {
                    backoff
                };
                tokio::time::sleep(wait).await;
                backoff = (backoff * 2).min(RECONNECT_MAX);
            }
        });
    }
    rx
}

async fn subscribe(name: &str, url: &str, source: usize, tx: &watch::Sender<Option<Head>>) -> Result<()> {
    let provider = ProviderBuilder::new().connect_ws(WsConnect::new(url)).await?;
    let mut stream = provider.subscribe_blocks().await?.into_stream();
    info!(endpoint = name, "ws newHeads subscribed");
    loop {
        // alloy's pubsub backend can give up reconnecting while the subscription stream neither
        // ends nor yields (seen 2026-10-17 19:46 UTC: every head then came from slower HTTP polling
        // for 8 h). A silent socket is treated as dead and rebuilt from scratch.
        let h = match tokio::time::timeout(WS_STALL, stream.next()).await {
            Ok(Some(h)) => h,
            Ok(None) => return Ok(()),
            Err(_) => anyhow::bail!("no head for {}s", WS_STALL.as_secs()),
        };
        let head = Head {
            number: h.number,
            timestamp: h.timestamp,
            base_fee: h.base_fee_per_gas.unwrap_or(0) as u128,
            seen_at: Instant::now(),
            source,
        };
        // Only move forward: the first endpoint to deliver a block number wins.
        tx.send_if_modified(|cur| {
            if cur.is_none_or(|c| head.number > c.number) {
                *cur = Some(head);
                true
            } else {
                false
            }
        });
    }
}
