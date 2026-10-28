//! The checks every bot makes before it sends, and the cost figures they need: the kill-switch
//! file, a daily budget for gas burned by reverted txs, deadlines in blocks, gas and L1-fee costs.
//! The executor's failure cooldown (`executor::FailurePolicy`) is the third gate; `Gates::check`
//! applies all three.

use alloy::{
    primitives::{Address, Bytes, U256, address},
    providers::Provider,
    rpc::types::TransactionRequest,
    sol,
    sol_types::SolCall,
};
use anyhow::Result;

/// The kill switch: while this file exists nothing is sent. An empty path = no kill switch.
pub fn kill_switch_on(path: &str) -> bool {
    !path.is_empty() && std::path::Path::new(path).exists()
}

/// Gas burned by failed (reverted) txs per UTC day. Successful sends are not charged: they paid for
/// their own gas, and charging them would block profitable sends on a busy day.
#[derive(Debug, Default, Clone)]
pub struct DailyBudget {
    day: u64,
    spent_usd: f64,
}

impl DailyBudget {
    fn roll(&mut self, unix_secs: u64) {
        let day = unix_secs / 86_400;
        if day != self.day {
            (self.day, self.spent_usd) = (day, 0.0);
        }
    }

    /// The actual cost of a tx that reverted on-chain.
    pub fn charge(&mut self, unix_secs: u64, usd: f64) {
        self.roll(unix_secs);
        self.spent_usd += usd;
    }

    /// Would a send that may cost `usd` (if it reverts) stay within `cap_usd` today?
    pub fn allows(&mut self, unix_secs: u64, usd: f64, cap_usd: f64) -> bool {
        self.roll(unix_secs);
        self.spent_usd + usd <= cap_usd
    }

    /// Spent today (as of the last charge or check).
    pub fn spent_usd(&mut self, unix_secs: u64) -> f64 {
        self.roll(unix_secs);
        self.spent_usd
    }
}

/// Why a send was refused by the gates.
#[derive(Debug, Clone, PartialEq)]
pub enum Refusal {
    KillSwitch,
    Cooldown,
    Budget { spent_usd: f64, worst_usd: f64, cap_usd: f64 },
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Refusal::KillSwitch => write!(f, "kill switch file present"),
            Refusal::Cooldown => write!(f, "sending paused (failure cooldown)"),
            Refusal::Budget { spent_usd, worst_usd, cap_usd } => write!(f, "daily gas budget: spent {spent_usd:.4} + {worst_usd:.4} > {cap_usd}"),
        }
    }
}

/// The gates every send goes through: kill-switch file, the executor's failure cooldown, and the
/// daily budget for reverted gas (the send's worst case if it reverts must fit).
pub struct Gates<'a> {
    pub kill_switch_file: &'a str,
    pub budget: &'a mut DailyBudget,
    pub daily_cap_usd: f64,
    pub now_unix: u64,
}

impl Gates<'_> {
    /// `halted` = the executor's failure cooldown; `worst_usd` = what the send costs if it reverts.
    pub fn check(&mut self, halted: bool, worst_usd: f64) -> Result<(), Refusal> {
        if kill_switch_on(self.kill_switch_file) {
            return Err(Refusal::KillSwitch);
        }
        if halted {
            return Err(Refusal::Cooldown);
        }
        if !self.budget.allows(self.now_unix, worst_usd, self.daily_cap_usd) {
            return Err(Refusal::Budget { spent_usd: self.budget.spent_usd(self.now_unix), worst_usd, cap_usd: self.daily_cap_usd });
        }
        Ok(())
    }
}

/// A deadline in seconds as blocks at the observed block time (works across a block-time change,
/// e.g. 2 s -> 200 ms, without config edits).
pub fn deadline_blocks(deadline_secs: f64, block_time_secs: f64) -> u64 {
    (deadline_secs / block_time_secs.max(0.05)).ceil().max(1.0) as u64
}

/// Gas cost in USD of `gas` at `base_fee + priority`.
pub fn gas_cost_usd(gas: u64, base_fee_wei: u128, priority_wei: u128, eth_usd: f64) -> f64 {
    gas as f64 * (base_fee_wei + priority_wei) as f64 / 1e18 * eth_usd
}

sol! {
    interface IGasPriceOracle { function getL1FeeUpperBound(uint256 unsignedTxSize) external view returns (uint256); }
}
/// OP-stack GasPriceOracle predeploy (Base, Optimism and other OP-stack chains).
pub const GAS_PRICE_ORACLE: Address = address!("420000000000000000000000000000000000000F");

/// Calldata for the L1 data fee (upper bound) of a tx carrying `calldata_len` bytes (+100 for the
/// rest of the signed tx).
pub fn l1_fee_call(calldata_len: usize) -> Bytes {
    IGasPriceOracle::getL1FeeUpperBoundCall { unsignedTxSize: U256::from(calldata_len + 100) }.abi_encode().into()
}

/// The L1 data fee (upper bound) of a tx carrying `calldata_len` bytes: paid whatever the priority.
pub async fn l1_fee_wei<P: Provider>(p: &P, calldata_len: usize) -> Result<U256> {
    let out = p.call(TransactionRequest::default().to(GAS_PRICE_ORACLE).input(l1_fee_call(calldata_len).into())).await?;
    Ok(IGasPriceOracle::getL1FeeUpperBoundCall::abi_decode_returns(&out)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn daily_budget_caps_failed_spend_and_resets_at_utc_midnight() {
        let mut b = DailyBudget::default();
        assert!(b.allows(10, 4.0, 5.0));
        b.charge(10, 4.0);
        assert!(!b.allows(20, 1.5, 5.0), "4 + 1.5 > 5");
        assert!(b.allows(20, 1.0, 5.0));
        assert!(b.allows(86_400 + 1, 4.9, 5.0), "a new UTC day starts from zero");
    }

    #[test]
    fn deadlines_follow_the_block_time() {
        assert_eq!(deadline_blocks(4.0, 2.0), 2);
        assert_eq!(deadline_blocks(4.0, 0.2), 20, "200 ms blocks");
        assert_eq!(deadline_blocks(0.1, 2.0), 1, "at least one block");
    }

    #[test]
    fn gates_refuse_on_kill_switch_cooldown_and_budget_in_that_order() {
        let kill = std::env::temp_dir().join(format!("onchain-core-kill-{}", std::process::id()));
        let _ = std::fs::remove_file(&kill);
        let mut budget = DailyBudget::default();
        budget.charge(0, 4.9);
        let mut g = Gates { kill_switch_file: kill.to_str().unwrap(), budget: &mut budget, daily_cap_usd: 5.0, now_unix: 0 };
        assert_eq!(g.check(false, 0.05), Ok(()));
        assert!(matches!(g.check(false, 0.2), Err(Refusal::Budget { .. })));
        assert_eq!(g.check(true, 0.05), Err(Refusal::Cooldown));
        std::fs::write(&kill, b"stop").unwrap();
        assert_eq!(g.check(true, 0.2), Err(Refusal::KillSwitch));
        let _ = std::fs::remove_file(&kill);
        assert!(!kill_switch_on(""), "no path = no kill switch");
    }
}
