//! Shared plumbing for on-chain bots, used by `liquidation-framework` and `arbitrage-framework`:
//!
//! - `rpc`: a multi-endpoint JSON-RPC pool (race / failover by priority, method and log-range
//!   limits, lag handling, capacity vs deterministic errors; logs show endpoint names only);
//! - `heads`, `flashblocks`: the new-block signal and Base's Flashblocks stream;
//! - `chain`, `num`: Multicall3 reads, unit and mul-div helpers;
//! - `sim`: one call simulated with `eth_simulateV1` (next-block overrides, fee-priced, L1 fee);
//! - `executor`: keystore signing, broadcast to every endpoint, local + shared nonce, several txs
//!   in flight, stuck-tx cancel, failure cooldown; `actor` runs it as its own task;
//! - `gates`: kill switch, daily budget for reverted gas, deadlines, gas and L1-fee costs;
//! - `bid`: priority-fee bids against the observed winning bids;
//! - `config`: the config sections every bot flattens into its own.
//!
//! Every bot that signs for one wallet must use the same version of the shared nonce file format
//! (`executor::SHARED_NONCE_VERSION`): a bot that finds another format refuses to send.

pub mod actor;
pub mod bid;
pub mod chain;
pub mod config;
pub mod executor;
pub mod flashblocks;
pub mod gates;
pub mod heads;
pub mod num;
pub mod rpc;
pub mod sim;
