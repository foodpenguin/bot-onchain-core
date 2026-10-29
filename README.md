# bot-onchain-core

[繁體中文](README.zh-TW.md)

The plumbing that on-chain bots share, as a Rust library ([alloy](https://github.com/alloy-rs/alloy)):
RPC pool, head and Flashblocks streams, exact simulation with `eth_simulateV1`, keystore signing,
broadcast, nonce coordination between bots, the executor (as its own task), send gates and
priority-fee bidding against observed competition. The crate is `onchain-core`; it is what
[Debt-keeper](https://github.com/foodpenguin/Debt-keeper) (liquidations) and
[Arb-seeker](https://github.com/foodpenguin/Arb-seeker) (arbitrage) are built on. It knows nothing
about lending protocols or arbitrage strategies.

> **Status:** experimental. Nothing here is financial advice. Code that signs and sends is tested
> on local anvil chains; run your bot in a shadow / observe mode first.

## Modules

| Module | What it does |
|---|---|
| `rpc` | Multi-endpoint JSON-RPC pool for alloy: race or fail over by priority, per-endpoint `skip_methods` and log-range limits, lagging nodes tried elsewhere without a cooldown, capacity errors (matched by phrases) fail over, deterministic errors of the call itself never do. Logs show endpoint names, never URLs (they carry API keys). |
| `heads` | New-head signal: WS `newHeads` on several endpoints at once (first one wins), HTTP polling fallback. |
| `flashblocks` | Base Flashblocks: message decoding, a reconnecting reader (`run_stream`), and every flashblock in arrival order (`spawn_raw`). Nothing else depends on it (Base's "Denim" upgrade removes Flashblocks). |
| `chain`, `num` | Multicall3 batched reads; unit conversion and full-precision mul-div. |
| `sim` | One call simulated exactly as it would run: `eth_simulateV1` on a block plus state overrides, optionally as the next block, priced at the real fee (a sender balance override keeps nodes from refusing it), with the gas used, the L1 data fee, and an optional probe read in the same state. |
| `executor` | Signing from an encrypted keystore (password from an env var removed after reading, or a prompt), broadcast to every endpoint over warm connections, local nonce plus a per-wallet shared nonce file, several txs in flight with consecutive nonces (`TxKey` = what a tx is about; two with the same key never fly together; an `exclusive_group` send is refused by the executor itself while its group has one in flight; these refusals are a typed `SendRefused`, so callers can tell a lost race from a failure), dropped / stuck txs replaced by a 0-value self-transfer, failure cooldown. |
| `actor` | The executor as its own task: `ExecHandle` reads its state synchronously and sends as messages; receipts are polled on every announced block; settled txs come back on a channel. Planning never waits for a receipt; a block loop may wait briefly (`wait_polled`, at most `RECEIPT_WAIT`) for a new head's receipts, so a tx included in it no longer counts as in flight. |
| `gates` | The checks before every send: kill-switch file, daily budget for gas burned by reverted txs (txs in flight reserve their worst case until they settle, `reserve` / `release`, so sends at once cannot overrun it together; a key that already holds a reservation is refused, never overwritten), plus deadlines in blocks, gas and L1-fee costs (OP-stack GasPriceOracle). |
| `bid` | Priority fees against competition: `WinningBids` keeps what past winners paid per USD of gross profit (per group, over a time window); `BidCurve` bids just above the predicted competitor, never more than a share of the net that grows with the size of the opportunity, never below the base priority. |
| `config` | The config sections every bot `#[serde(flatten)]`s into its own: `RpcSettings` (endpoints and the `.env` holding their keys) and `SendSettings` (wallet, mode, keystore, broadcast, kill switch, nonce file, budget, failure policy, `max_inflight`). |

## Using it

Depend on a tagged release (the frameworks pin one the same way):

```toml
[dependencies]
onchain-core = { git = "https://github.com/foodpenguin/bot-onchain-core", tag = "v0.1.0" }
```

To build a framework against a local checkout of this repository instead (e.g. while changing
both), override the dependency from the framework's `bot/` directory:

```
cargo test --config 'patch."https://github.com/foodpenguin/bot-onchain-core".onchain-core.path="../../bot-onchain-core"'
```

A minimal sender:

```rust
use onchain_core::{actor, executor::{Broadcaster, Executor}};

let signer = executor::load_signer(&keystore, "BOT_KEYSTORE_PASSWORD")?; // before the async runtime
// ... inside the runtime:
let bcast = Broadcaster::new(Broadcaster::client()?, endpoints);
let mut exec = Executor::new(provider.clone(), signer, chain_id, bcast, settings.failure_policy())
    .with_shared_nonce(settings.nonce_path(address))
    .with_max_inflight(settings.max_inflight);
exec.sync_nonce().await?;
exec.spawn_nonce_watch(executor::NONCE_WATCH_EVERY);
let (handle, mut settled) = actor::spawn(exec, blocks_rx); // blocks_rx: watch::Receiver<u64>
let inflight = handle.send(request).await?;               // check gates::Gates first
```

## The shared nonce file

Every bot that signs for one wallet locks one file per wallet around sign + broadcast
(`~/.local/state/onchain-bots/nonce-<wallet>` by default) and records the next nonce in it, in the
format `v1 <next nonce> <unix ms>`. A bot that finds a format it does not know refuses to send
rather than guess (`executor::SHARED_NONCE_VERSION`). The one exception is the unversioned
`<next nonce> <unix ms>` of earlier releases: a stale one (older than 30 s) is treated as empty and
overwritten, so an upgrade needs no manual cleanup; a fresh one means an older bot may be sending
right now, and is refused like any other. Bots built on different, incompatible versions must not
share a wallet.

## Checks

```
cargo check --all-targets   # 0 warnings
cargo clippy --all-targets   # 0 warnings
cargo test                  # unit tests, incl. a two-process test of the nonce lock
cargo test -- --ignored     # the executor task against a local anvil chain (needs anvil)
```

## License

[GNU Affero General Public License v3.0 or later](LICENSE). This software is provided without any
warranty. See [SECURITY.md](SECURITY.md) to report a vulnerability and
[CONTRIBUTING.md](CONTRIBUTING.md) to contribute.
