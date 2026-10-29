//! The executor as its own task. Callers never hold the `Executor`: they read its state through an
//! `ExecHandle` (cheap, synchronous) and send through it (a message to the task, answered when the
//! broadcast is accepted). Receipts are polled by the task itself on every new block, so neither
//! planning nor the block loop waits for them; settled txs arrive on a channel.
//!
//! Sends are handled one at a time, in arrival order: nonces are consecutive, and the shared nonce
//! file is held from signing to broadcast. A caller that wants several sends in parallel just
//! issues them from several tasks; each waits only for its own turn.

use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use alloy::{
    primitives::{Address, U256},
    providers::Provider,
};
use anyhow::{Result, anyhow};
use tokio::sync::{mpsc, oneshot, watch};
use tracing::warn;

use crate::executor::{Executor, Inflight, Outcome, SendRequest, TxKey};

/// How long a block loop waits for the receipt poll of a new head (`ExecHandle::wait_polled`)
/// before it goes on anyway.
pub const RECEIPT_WAIT: Duration = Duration::from_millis(500);

/// The executor's state as of its last change, plus the live balance reading.
#[derive(Clone)]
pub struct ExecView {
    pub address: Address,
    pub inflight: Vec<Inflight>,
    pub max_inflight: usize,
    /// The latest block whose receipts the task has checked (every block counts while nothing is
    /// in flight).
    pub polled_block: u64,
    cool_until: Option<Instant>,
    balance: Arc<Mutex<Option<U256>>>,
}

impl ExecView {
    /// In the failure cooldown.
    pub fn halted(&self) -> bool {
        self.cool_until.is_some_and(|t| Instant::now() < t)
    }
    /// Not halted and below max_inflight.
    pub fn can_send(&self) -> bool {
        !self.halted() && self.inflight.len() < self.max_inflight
    }
    pub fn is_inflight(&self, key: TxKey) -> bool {
        self.inflight.iter().any(|i| i.key == key)
    }
    pub fn is_group_inflight(&self, group: u64) -> bool {
        self.inflight.iter().any(|i| i.key.group == group)
    }
    /// The last balance reading minus what the txs in flight may still burn at most (None = no
    /// reading yet).
    pub fn spendable(&self) -> Option<U256> {
        let b = (*self.balance.lock().unwrap())?;
        let reserved: u128 = self.inflight.iter().map(|i| i.gas_limit as u128 * i.max_fee_wei).sum();
        Some(b.saturating_sub(U256::from(reserved)))
    }
}

/// A tx that settled, as the task reports it.
#[derive(Debug, Clone)]
pub struct Settled {
    pub inflight: Inflight,
    pub outcome: Outcome,
    /// This outcome put the executor into its failure cooldown.
    pub cooldown_started: bool,
}

enum Cmd {
    Send(SendRequest, oneshot::Sender<Result<Inflight>>),
}

/// Sends to, and reads the state of, the executor task. Clone it freely.
#[derive(Clone)]
pub struct ExecHandle {
    cmd: mpsc::Sender<Cmd>,
    view: watch::Receiver<ExecView>,
}

impl ExecHandle {
    pub fn view(&self) -> ExecView {
        self.view.borrow().clone()
    }
    pub fn address(&self) -> Address {
        self.view.borrow().address
    }
    pub fn can_send(&self) -> bool {
        self.view.borrow().can_send()
    }
    pub fn halted(&self) -> bool {
        self.view.borrow().halted()
    }
    pub fn is_inflight(&self, key: TxKey) -> bool {
        self.view.borrow().is_inflight(key)
    }
    pub fn is_group_inflight(&self, group: u64) -> bool {
        self.view.borrow().is_group_inflight(group)
    }
    pub fn spendable(&self) -> Option<U256> {
        self.view.borrow().spendable()
    }

    /// Waits (at most `timeout`) until the task has checked the receipts of `block`, so that a tx
    /// included in it no longer counts as in flight. Returns at once when nothing is in flight.
    /// A block loop calls it after announcing a head and before deciding whether it can send.
    pub async fn wait_polled(&self, block: u64, timeout: Duration) {
        let mut rx = self.view.clone();
        let _ = tokio::time::timeout(timeout, rx.wait_for(|v| v.inflight.is_empty() || v.polled_block >= block)).await;
    }

    /// Signs and broadcasts `r` (the executor's own checks apply: cooldown, max_inflight, same
    /// key in flight, the shared nonce lock). Returns once an endpoint accepted it.
    pub async fn send(&self, r: SendRequest) -> Result<Inflight> {
        let (tx, rx) = oneshot::channel();
        self.cmd.send(Cmd::Send(r, tx)).await.map_err(|_| anyhow!("executor task stopped"))?;
        rx.await.map_err(|_| anyhow!("executor task stopped"))?
    }
}

/// Starts the executor task. `blocks` = the latest block number (each change triggers a receipt
/// poll while anything is in flight). The task ends when every handle is dropped and nothing is
/// left in flight.
pub fn spawn<P: Provider + Send + Sync + 'static>(mut exec: Executor<P>, mut blocks: watch::Receiver<u64>) -> (ExecHandle, mpsc::UnboundedReceiver<Settled>) {
    let view_of = |e: &Executor<P>, polled_block: u64| ExecView { address: e.address, inflight: e.inflight.clone(), max_inflight: e.max_inflight(), polled_block, cool_until: e.cool_until(), balance: e.balance_cell() };
    let mut polled = *blocks.borrow();
    let (view_tx, view_rx) = watch::channel(view_of(&exec, polled));
    let (cmd_tx, mut cmd_rx) = mpsc::channel::<Cmd>(64);
    let (settled_tx, settled_rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        let (mut cmds_open, mut blocks_open) = (true, true);
        loop {
            tokio::select! {
                // Sends first: a waiting send is an opportunity, a receipt can wait a moment.
                biased;
                c = cmd_rx.recv(), if cmds_open => match c {
                    Some(Cmd::Send(r, reply)) => {
                        let res = exec.send(r).await;
                        // The view first: a caller that reads it right after its send sees the tx.
                        view_tx.send_replace(view_of(&exec, polled));
                        let _ = reply.send(res);
                    }
                    None => cmds_open = false,
                },
                r = blocks.changed(), if blocks_open => {
                    if r.is_err() {
                        blocks_open = false;
                        continue;
                    }
                    let block = *blocks.borrow_and_update();
                    if !exec.inflight.is_empty() {
                        let was_cooling = exec.halted();
                        match exec.poll(block).await {
                            Ok(done) => {
                                let n = done.len();
                                for (i, (inflight, outcome)) in done.into_iter().enumerate() {
                                    let cooldown_started = i + 1 == n && !was_cooling && exec.halted();
                                    let _ = settled_tx.send(Settled { inflight, outcome, cooldown_started });
                                }
                            }
                            Err(e) => warn!(error = %e, "receipt poll failed"),
                        }
                    }
                    // Checked (or a failed check given up on): waiters move on either way.
                    polled = polled.max(block);
                }
                else => break,
            }
            view_tx.send_replace(view_of(&exec, polled));
            if !cmds_open && (exec.inflight.is_empty() || !blocks_open) {
                break;
            }
        }
    });
    (ExecHandle { cmd: cmd_tx, view: view_rx }, settled_rx)
}

#[cfg(test)]
mod tests {
    //! Against a local anvil chain (no fork, no network): run with `cargo test -- --ignored`.
    use alloy::{
        node_bindings::Anvil,
        primitives::{B256, Bytes},
        providers::{Provider, ProviderBuilder, ext::AnvilApi},
        signers::local::PrivateKeySigner,
    };

    use super::*;
    use crate::executor::{Broadcaster, FailurePolicy, MinProfitSlot, Payload};

    #[tokio::test]
    #[ignore = "needs anvil on PATH; run with --ignored"]
    async fn the_task_sends_in_parallel_polls_receipts_and_reports_settled_txs() {
        let anvil = Anvil::new().arg("--no-mining").spawn();
        let url = anvil.endpoint_url();
        let p = ProviderBuilder::new().connect_http(url.clone());
        let signer: PrivateKeySigner = anvil.keys()[0].clone().into();
        let to = anvil.addresses()[1];
        let bcast = Broadcaster::new(reqwest::Client::new(), vec![("anvil".into(), url.to_string())]);
        let policy = FailurePolicy { max_in_window: 9, window: std::time::Duration::from_secs(60), cooldown: std::time::Duration::from_secs(60) };
        let exec = Executor::new(p.clone(), signer, anvil.chain_id(), bcast, policy).with_max_inflight(3);
        let (blocks_tx, blocks) = watch::channel(0u64);
        let (h, mut settled) = spawn(exec, blocks);

        let base_fee = p.get_block_by_number(alloy::eips::BlockNumberOrTag::Latest).await.unwrap().unwrap().header.base_fee_per_gas.unwrap_or(1) as u128;
        // 32 bytes of calldata to an EOA; "minProfit" is patched into all of it.
        let req = |id: u8, exclusive_group: bool| SendRequest {
            payload: Payload { to, calldata: Bytes::from(vec![0u8; 32]), min_profit: MinProfitSlot { offset: 0, width: 32 } },
            min_profit: U256::from(id),
            gas_limit: 60_000,
            base_fee_wei: base_fee,
            priority_fee_wei: 1_000_000,
            key: TxKey::new(1, B256::with_last_byte(id)),
            exclusive_group,
            block: 0,
            deadline_block: 100,
        };
        // Two sends issued at once from two tasks: both go out, with consecutive nonces.
        let (a, b) = tokio::join!(h.send(req(1, false)), h.send(req(2, false)));
        let (a, b) = (a.unwrap(), b.unwrap());
        assert_eq!(b.nonce.max(a.nonce) - b.nonce.min(a.nonce), 1);
        let v = h.view();
        assert_eq!(v.inflight.len(), 2, "the view shows both right after the sends");
        assert!(v.is_group_inflight(1) && v.can_send());
        // A send that wants its group to itself is refused by the task, whatever the caller checked.
        let refused = |e: anyhow::Error| e.downcast_ref::<crate::executor::SendRefused>().cloned();
        assert_eq!(refused(h.send(req(3, true)).await.unwrap_err()), Some(crate::executor::SendRefused::GroupInflight));
        h.send(req(3, false)).await.unwrap();
        assert!(!h.can_send(), "max_inflight = 3 reached");
        assert_eq!(refused(h.send(req(4, false)).await.unwrap_err()), Some(crate::executor::SendRefused::MaxInflight(3)), "the executor's own limit holds");

        // Mine, announce the block: the task polls and reports all three, and a block loop that
        // waits for that poll sees them settled.
        p.anvil_mine(Some(1), None).await.unwrap();
        let mined = p.get_block_number().await.unwrap();
        blocks_tx.send_replace(mined);
        h.wait_polled(mined, std::time::Duration::from_secs(10)).await;
        assert!(h.view().inflight.is_empty() && h.can_send() && h.view().polled_block == mined, "settled txs leave the view");
        let mut got = vec![];
        while got.len() < 3 {
            let s = tokio::time::timeout(std::time::Duration::from_secs(10), settled.recv()).await.expect("settled in time").unwrap();
            assert!(matches!(s.outcome, Outcome::Included { success: true, .. }), "{:?}", s.outcome);
            got.push(s.inflight.key.id);
        }
        got.sort();
        assert_eq!(got, vec![B256::with_last_byte(1), B256::with_last_byte(2), B256::with_last_byte(3)]);
    }
}
