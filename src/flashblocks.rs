//! Flashblocks stream (Base, until Denim): ~200 ms sub-blocks pushed over WebSocket as
//! brotli-compressed JSON. Message `index` 0 opens a block (`base`), 1..=10 add transactions
//! (`diff.transactions`, raw EIP-2718 bytes). `spawn_raw` streams every flashblock in arrival
//! order so a strategy can quote and act on the in-progress block before it is sealed;
//! `run_stream` is the reconnecting reader underneath, for consumers that decode messages
//! themselves (e.g. watching oracle transmissions).
//!
//! Base's planned "Denim" upgrade removes Flashblocks in favour of 200 ms blocks: nothing else in
//! this crate depends on this module.

use std::{io::Read, time::{Duration, Instant}};

use alloy::primitives::Bytes;
use anyhow::{Context, Result};
use futures::StreamExt;
use serde::Deserialize;
use tokio::sync::mpsc;
use tracing::{info, warn};

#[derive(Deserialize)]
struct Msg {
    #[serde(default)]
    index: u64,
    #[serde(default)]
    base: Option<MsgBase>,
    #[serde(default)]
    diff: Option<MsgDiff>,
    #[serde(default)]
    metadata: Option<MsgMeta>,
}
#[derive(Deserialize)]
struct MsgBase {
    block_number: String,
    #[serde(default)]
    timestamp: Option<String>,
    #[serde(default)]
    base_fee_per_gas: Option<String>,
}
#[derive(Deserialize)]
struct MsgDiff {
    #[serde(default)]
    transactions: Vec<Bytes>,
}
#[derive(Deserialize)]
struct MsgMeta {
    block_number: u64,
}

/// One decoded flashblock.
pub struct Decoded {
    pub block: u64,
    pub index: u64,
    pub txs: Vec<Bytes>,
    /// Block timestamp and base fee, carried by index 0 only.
    pub timestamp: Option<u64>,
    pub base_fee: Option<u128>,
}

pub fn decode_message(compressed: &[u8]) -> Result<Decoded> {
    let mut s = String::new();
    brotli::Decompressor::new(compressed, 4096).read_to_string(&mut s).context("brotli")?;
    let m: Msg = serde_json::from_str(&s).context("flashblock json")?;
    let block = match (&m.metadata, &m.base) {
        (Some(md), _) => md.block_number,
        (None, Some(b)) => u64::from_str_radix(b.block_number.trim_start_matches("0x"), 16)?,
        _ => anyhow::bail!("flashblock without block number"),
    };
    let timestamp = m.base.as_ref().and_then(|b| b.timestamp.as_deref()).and_then(|t| u64::from_str_radix(t.trim_start_matches("0x"), 16).ok());
    let base_fee = m.base.as_ref().and_then(|b| b.base_fee_per_gas.as_deref()).and_then(|t| u128::from_str_radix(t.trim_start_matches("0x"), 16).ok());
    Ok(Decoded { block, index: m.index, txs: m.diff.map(|d| d.transactions).unwrap_or_default(), timestamp, base_fee })
}

/// Reads the stream at `url` in a background task, (re)connecting with backoff, and calls `on_msg`
/// with every decoded message and its arrival time. `stream` names it in logs.
pub fn run_stream(url: String, stream: &'static str, mut on_msg: impl FnMut(Decoded, Instant) + Send + 'static) {
    tokio::spawn(async move {
        let mut backoff = Duration::from_secs(1);
        loop {
            match tokio_tungstenite::connect_async(url.as_str()).await {
                Ok((mut ws, _)) => {
                    info!(stream, "flashblocks stream connected");
                    backoff = Duration::from_secs(1);
                    while let Ok(Some(msg)) = tokio::time::timeout(Duration::from_secs(5), ws.next()).await {
                        let Ok(tokio_tungstenite::tungstenite::Message::Binary(b)) = msg else {
                            if msg.is_err() {
                                break;
                            }
                            continue;
                        };
                        let at = Instant::now();
                        match decode_message(&b) {
                            Ok(d) => on_msg(d, at),
                            Err(e) => warn!(stream, error = %e, "flashblock decode failed"),
                        }
                    }
                    warn!(stream, "flashblocks stream stalled or closed; reconnecting");
                }
                Err(e) => warn!(stream, error = %e, backoff_s = backoff.as_secs(), "flashblocks connect failed"),
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_secs(30));
        }
    });
}

/// One flashblock as it arrived, for consumers that look at every transaction.
#[derive(Debug, Clone)]
pub struct Flashblock {
    pub block: u64,
    pub index: u64,
    pub txs: Vec<Bytes>,
    /// Carried by index 0 only.
    pub timestamp: Option<u64>,
    pub base_fee: Option<u128>,
    pub seen: Instant,
}

/// Every flashblock in arrival order, (re)connecting in the background. A message that finds the
/// channel full is dropped (and logged): the consumer sees the gap in `index`.
pub fn spawn_raw(url: String) -> mpsc::Receiver<Flashblock> {
    let (tx, rx) = mpsc::channel(256);
    run_stream(url, "raw", move |Decoded { block, index, txs, timestamp, base_fee }, seen| {
        if tx.try_send(Flashblock { block, index, txs, timestamp, base_fee, seen }).is_err() {
            warn!(block, index, "flashblock channel full; message dropped");
        }
    });
    rx
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    // Any byte blob; the decoder carries transactions through verbatim.
    fn sample_tx() -> Vec<u8> {
        vec![0x02, 0xf8, 0x6c, 0x01, 0x80, 0x84, 0x3b, 0x9a, 0xca, 0x00]
    }

    #[test]
    fn decodes_compressed_flashblock_messages() {
        let raw = sample_tx();
        let json = serde_json::json!({
            "payload_id": "0x01", "index": 3,
            "diff": { "transactions": [format!("0x{}", alloy::hex::encode(&raw))] },
            "metadata": { "block_number": 52738345u64 }
        })
        .to_string();
        let mut comp = Vec::new();
        {
            let mut w = brotli::CompressorWriter::new(&mut comp, 4096, 5, 22);
            w.write_all(json.as_bytes()).unwrap();
        }
        let Decoded { block, index, txs, timestamp, base_fee } = decode_message(&comp).unwrap();
        assert_eq!((timestamp, base_fee), (None, None), "only index 0 carries the block timestamp and base fee");
        assert_eq!((block, index, txs.len()), (52738345, 3, 1));
        assert_eq!(&txs[0][..], &raw[..]);
        // index 0 carries the block number in `base` (hex) instead of `metadata`.
        let open = serde_json::json!({ "payload_id": "0x01", "index": 0, "base": { "block_number": "0x324b94", "timestamp": "0x6ad3cf8f", "base_fee_per_gas": "0x5b8d80" }, "diff": { "transactions": [] } }).to_string();
        let mut comp = Vec::new();
        {
            let mut w = brotli::CompressorWriter::new(&mut comp, 4096, 5, 22);
            w.write_all(open.as_bytes()).unwrap();
        }
        let d = decode_message(&comp).unwrap();
        assert_eq!((d.block, d.timestamp, d.base_fee), (0x324b94, Some(0x6ad3cf8f), Some(6_000_000)));
    }
}
