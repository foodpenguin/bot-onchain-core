//! One call simulated exactly as it would run: `eth_simulateV1` on a chosen block plus state
//! overrides, optionally as the next block (number and timestamp), priced like the real tx, with
//! the gas actually used and the L1 data fee of its calldata.

use alloy::{
    eips::BlockId,
    network::TransactionBuilder,
    primitives::{Address, Bytes, U256},
    providers::Provider,
    rpc::types::{TransactionRequest, state::StateOverride},
    sol_types::SolCall,
};
use anyhow::{Context, Result};

use crate::gates::{GAS_PRICE_ORACLE, IGasPriceOracle, l1_fee_call};

/// The fee fields a simulated call carries: what the real tx would bid. Contracts may read
/// tx.gasprice (priority-fee taxes, dynamic fees), so a simulation at gas price 0 can overstate the
/// profit.
#[derive(Debug, Clone, Copy)]
pub struct SimFee {
    pub max_fee_per_gas: u128,
    pub max_priority_fee_per_gas: u128,
}

/// Execute a simulation as block `number` with timestamp `time`.
#[derive(Debug, Clone, Copy)]
pub struct SimAt {
    pub number: u64,
    pub time: u64,
}

/// One simulated call (see `simulate_call`).
pub struct RawSim {
    pub return_data: Bytes,
    pub gas_used: u64,
    pub l1_fee_wei: U256,
    /// Number of the block the node simulated (None if it did not say). On a "pending" base it
    /// shows whether the node really built on the in-progress block or on a stale one.
    pub block_number: Option<u64>,
    /// The probe's 32-byte answer, read in the same simulated state just before the call.
    pub probe: Option<U256>,
}

/// A simulation that ran and reverted; `block_number` = the block the node simulated.
#[derive(Debug)]
pub struct SimReverted {
    pub message: String,
    pub block_number: Option<u64>,
    pub probe: Option<U256>,
}

impl std::fmt::Display for SimReverted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "sim reverted: {}", self.message)
    }
}

impl std::error::Error for SimReverted {}

/// A view call made in the same simulated state just before the call under test (e.g. an oracle's
/// `price()`): same request, so same node and state. Its answer must be one 32-byte word.
#[derive(Debug, Clone)]
pub struct Probe {
    pub to: Address,
    pub data: Bytes,
}

/// Runs `calldata` from `from` to `to` on top of `block`'s state plus `ov` with eth_simulateV1, as
/// the next block when `at` is given, and returns what it returned with the gas actually used and
/// the L1 fee of its calldata. A revert is a `SimReverted` error.
///
/// One `eth_simulateV1` call returns both the result and the gas actually used. (eth_call +
/// eth_estimateGas cannot do this: some nodes ignore block overrides in estimateGas, so the
/// estimate would run at the wrong block.)
#[allow(clippy::too_many_arguments)]
pub async fn simulate_call<P: Provider>(p: &P, from: Address, to: Address, calldata: Bytes, block: BlockId, ov: &StateOverride, at: Option<SimAt>, fee: Option<SimFee>, probe: Option<Probe>) -> Result<RawSim> {
    let mut call = serde_json::json!({
        "from": from,
        "to": to,
        "data": calldata,
        "gas": "0x1c9c380", // 30M: generous ceiling, gasUsed is what we report
    });
    // Priced like the real tx: pools that read tx.gasprice (priority-fee taxes, dynamic fees) pay
    // out what they would at this bid, not at gas price 0. With validation off the simulated block's
    // base fee is 0, so tx.gasprice = the priority fee (what such taxes are levied on). Nodes still
    // check the sender can pay gas x maxFeePerGas (measured on Base, 2026-10-28), so the sender gets
    // a balance override.
    let mut ov = ov.clone();
    if let Some(f) = fee {
        call["maxFeePerGas"] = serde_json::json!(format!("0x{:x}", f.max_fee_per_gas));
        call["maxPriorityFeePerGas"] = serde_json::json!(format!("0x{:x}", f.max_priority_fee_per_gas));
        ov.entry(from).or_default().balance = Some(U256::from(u128::MAX));
    }
    let has_probe = probe.is_some();
    let mut calls: Vec<serde_json::Value> = probe.map(|q| serde_json::json!({ "from": from, "to": q.to, "data": q.data, "gas": "0x7a120" })).into_iter().collect();
    let idx = calls.len();
    calls.push(call);
    let mut block_calls = serde_json::json!({ "stateOverrides": ov, "calls": calls });
    if let Some(a) = at {
        block_calls["blockOverrides"] = serde_json::json!({ "number": format!("0x{:x}", a.number), "time": format!("0x{:x}", a.time) });
    }
    let payload = serde_json::json!({ "blockStateCalls": [block_calls], "validation": false });
    let tag = match block {
        BlockId::Number(alloy::eips::BlockNumberOrTag::Number(n)) => serde_json::json!(format!("0x{n:x}")),
        BlockId::Number(alloy::eips::BlockNumberOrTag::Pending) => serde_json::json!("pending"),
        _ => serde_json::json!("latest"),
    };
    // The L1 fee only depends on the calldata size: fetch it alongside the simulation.
    let (r, l1) = tokio::join!(p.raw_request::<_, serde_json::Value>("eth_simulateV1".into(), (payload, tag)), l1_fee_at(p, calldata.len(), block));
    let r = r.map_err(|e| anyhow::anyhow!("simulation unavailable (eth_simulateV1): {e}"))?;
    let block_number = r[0]["number"].as_str().and_then(|s| u64::from_str_radix(s.trim_start_matches("0x"), 16).ok());
    let probe = has_probe
        .then(|| {
            let pc = &r[0]["calls"][0];
            let data: Bytes = pc["returnData"].as_str().filter(|_| pc["status"].as_str() == Some("0x1"))?.parse().ok()?;
            (data.len() == 32).then(|| U256::from_be_slice(&data))
        })
        .flatten();
    let c = &r[0]["calls"][idx];
    if c["status"].as_str() != Some("0x1") {
        let msg = c["error"]["message"].as_str().unwrap_or("reverted");
        let data = c["returnData"].as_str().or_else(|| c["error"]["data"].as_str()).unwrap_or("");
        return Err(SimReverted { message: format!("{msg} {data}"), block_number, probe }.into());
    }
    let return_data: Bytes = c["returnData"].as_str().context("simulateV1: no returnData")?.parse()?;
    let gas_used = c["gasUsed"]
        .as_str()
        .and_then(|s| u64::from_str_radix(s.trim_start_matches("0x"), 16).ok())
        .context("simulateV1: no gasUsed")?;
    Ok(RawSim { return_data, gas_used, l1_fee_wei: l1.unwrap_or(U256::ZERO), block_number, probe })
}

/// Debug aid: does the call succeed under each fixed gas limit?
pub async fn probe_gas_call<P: Provider>(p: &P, from: Address, to: Address, calldata: Bytes, block: BlockId, ov: &StateOverride, limits: &[u64]) -> Vec<(u64, bool)> {
    let mut out = Vec::new();
    for &g in limits {
        let tx = TransactionRequest::default().with_from(from).with_to(to).with_input(calldata.clone()).with_gas_limit(g);
        out.push((g, p.call(tx).block(block).overrides(ov.clone()).await.is_ok()));
    }
    out
}

/// L1 data fee upper bound for a tx with this much calldata, read at `block`.
pub async fn l1_fee_at<P: Provider>(p: &P, calldata_len: usize, block: BlockId) -> Result<U256> {
    let tx = TransactionRequest::default().with_to(GAS_PRICE_ORACLE).with_input(l1_fee_call(calldata_len));
    let out = p.call(tx).block(block).await?;
    Ok(IGasPriceOracle::getL1FeeUpperBoundCall::abi_decode_returns(&out)?)
}
