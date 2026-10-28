//! Chain-neutral on-chain read helpers. Multicall3 is deployed at the same address on every
//! major EVM chain (Base, BNB, Arbitrum, ...), so batched reads live here, not in any protocol
//! module. `MULTICALL3` is a constant, not a config key: for a chain where Multicall3 sits at
//! another address (or is missing), change it here.

use alloy::{
    eips::BlockId,
    primitives::{Address, Bytes, address},
    providers::Provider,
    sol,
};
use anyhow::{Context, Result};

/// Canonical Multicall3 (same address on Base/BNB/Arbitrum/Optimism/Ethereum/...).
pub const MULTICALL3: Address = address!("cA11bde05977b3631167028862bE2a173976CA11");
const MULTICALL_CHUNK: usize = 400;
const MULTICALL_CONCURRENCY: usize = 6;

sol! {
    #[sol(rpc)]
    interface IMulticall3 {
        struct Call3 { address target; bool allowFailure; bytes callData; }
        struct Result { bool success; bytes returnData; }
        function aggregate3(Call3[] calldata calls) external payable returns (Result[] memory returnData);
    }
}

/// Runs calls through Multicall3 in chunks (up to MULTICALL_CONCURRENCY in flight);
/// failed sub-calls come back as None. Output order matches input order.
pub async fn multicall<P: Provider>(p: &P, calls: &[(Address, Bytes)], block: BlockId) -> Result<Vec<Option<Bytes>>> {
    use futures::{StreamExt, TryStreamExt, stream};
    let mc = IMulticall3::new(MULTICALL3, p);
    let chunks: Vec<Vec<IMulticall3::Call3>> = calls
        .chunks(MULTICALL_CHUNK)
        .map(|chunk| {
            chunk
                .iter()
                .map(|(target, data)| IMulticall3::Call3 { target: *target, allowFailure: true, callData: data.clone() })
                .collect()
        })
        .collect();
    let results: Vec<Vec<IMulticall3::Result>> = stream::iter(chunks)
        .map(|req| {
            let mc = &mc;
            async move { mc.aggregate3(req).block(block).call().await.context("multicall aggregate3") }
        })
        .buffered(MULTICALL_CONCURRENCY)
        .try_collect()
        .await?;
    Ok(results.into_iter().flatten().map(|r| r.success.then_some(r.returnData)).collect())
}
