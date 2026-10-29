# onchain-core — notes for AI coding assistants and contributors

The shared plumbing of the liquidation and arbitrage frameworks (Rust, alloy): RPC pool, heads,
Flashblocks, `eth_simulateV1` simulation, executor (+ actor task), send gates, bidding, config
sections. It must stay free of anything protocol- or strategy-specific.

## Commands
- `cargo check --all-targets` and `cargo clippy --all-targets` must stay at **0 warnings**; `cargo test` must pass;
  `cargo test -- --ignored` runs the executor task against a local anvil chain.
- A change here must also build and pass the tests of `../liquidation-framework/bot` and
  `../arbitrage-framework/bot` (they depend on this crate by path).

## Conventions
- Code comments in English; docs in English **and** Traditional Chinese (`*.zh-TW.md`), kept in sync.
- Match the surrounding code: naming, comment density, idioms.
- The shared nonce file format is versioned (`SHARED_NONCE_VERSION`); never change it in place.

## Safety rules
- Private keys only in an encrypted keystore. Never in code, config, `.env` or logs. Never ask for
  or handle a keystore password.
- RPC URLs contain API keys: logs and errors show endpoint names only.
- Automated tooling never sends mainnet transactions; anything that sends is tested on a local
  anvil chain.
