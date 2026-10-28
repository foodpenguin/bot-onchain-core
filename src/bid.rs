//! Priority-fee bidding against observed competition.
//!
//! On a chain ordered by priority fee, a race is won by the higher priority fee per gas. What the
//! winners of past races paid is public (their receipts), so the bid is "what it takes to win, if
//! the profit affords it":
//!
//! - `WinningBids` keeps, per group (a market, a pool set) and over a time window, how much
//!   priority per gas the winners paid per USD of gross profit they took (`ratio`). Gross profit,
//!   not the winner's gas: that is what is known from the outside, and it scales with the size of
//!   the opportunity the way competitors' bids do;
//! - `BidCurve::priority` predicts a competitor's priority fee for our opportunity (a quantile of
//!   the ratios x our gross), bids a little above it, and never more than a share of the net that
//!   grows with the size of the opportunity (up to `share_large` at `large_net_usd`: a big
//!   liquidation may bid most of its profit), nor below the base priority. Without enough
//!   observations it bids the affordable share, as a plain share-of-profit bidder would.

use std::collections::{HashMap, VecDeque};

/// One winning bid: when (unix seconds), and its priority fee per gas (wei) per USD of gross.
#[derive(Debug, Clone, Copy)]
struct Win {
    at: u64,
    ratio: f64,
}

/// Winning bids per group within the last `window_secs`.
#[derive(Debug, Default)]
pub struct WinningBids {
    window_secs: u64,
    groups: HashMap<String, VecDeque<Win>>,
}

impl WinningBids {
    pub fn new(window_secs: u64) -> Self {
        Self { window_secs, groups: HashMap::new() }
    }

    /// A race in `group` was won at `at` (unix seconds) with `priority_wei` per gas, taking
    /// `gross_usd`.
    pub fn observe(&mut self, group: &str, at: u64, priority_wei: u128, gross_usd: f64) {
        if gross_usd <= 0.0 {
            return;
        }
        let q = self.groups.entry(group.to_string()).or_default();
        q.push_back(Win { at, ratio: priority_wei as f64 / gross_usd });
        let keep_from = at.saturating_sub(self.window_secs);
        while q.front().is_some_and(|w| w.at < keep_from) {
            q.pop_front();
        }
    }

    /// Observations kept for `group`.
    pub fn len(&self, group: &str) -> usize {
        self.groups.get(group).map_or(0, |q| q.len())
    }

    pub fn is_empty(&self) -> bool {
        self.groups.values().all(|q| q.is_empty())
    }

    /// The `q`-quantile (0..=1) of the priority-per-USD ratios won in `group` at or after
    /// `now - window`, if it has at least `min_samples`; else over every group (a market
    /// with few liquidations borrows the others' competition), else None.
    pub fn ratio_quantile(&self, group: &str, now: u64, q: f64, min_samples: usize) -> Option<f64> {
        let from = now.saturating_sub(self.window_secs);
        let in_window = |v: &VecDeque<Win>| v.iter().filter(|w| w.at >= from).map(|w| w.ratio).collect::<Vec<f64>>();
        let own = self.groups.get(group).map(in_window).unwrap_or_default();
        let sample = if own.len() >= min_samples.max(1) { own } else { self.groups.values().flat_map(in_window).collect() };
        (sample.len() >= min_samples.max(1)).then(|| quantile(sample, q))
    }
}

fn quantile(mut v: Vec<f64>, q: f64) -> f64 {
    v.sort_by(f64::total_cmp);
    let i = ((v.len() - 1) as f64 * q.clamp(0.0, 1.0)).round() as usize;
    v[i]
}

/// How much of a profit a bid may use, and how far above the expected competition it goes.
#[derive(Debug, Clone, Copy)]
pub struct BidCurve {
    /// Priority fee of a tx that does not bid.
    pub base_wei: u128,
    /// Hard cap.
    pub max_wei: u128,
    /// Share of the net (above the floor) a small opportunity may bid...
    pub share: f64,
    /// ...rising linearly to this share at `large_net_usd` and above.
    pub share_large: f64,
    pub large_net_usd: f64,
    /// Which winning bid to beat: 0.9 = 90% of past winners bid at most this (per USD of gross).
    pub quantile: f64,
    /// Bid this much above the predicted competitor (bps).
    pub outbid_bps: u32,
    /// Observations needed before the competition is used.
    pub min_samples: usize,
}

/// A bid and why.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize)]
pub struct Bid {
    pub priority_wei: u128,
    /// The predicted competitor priority fee (None = not enough observations).
    pub competitor_wei: Option<u128>,
    /// The most the profit affords at the allowed share.
    pub affordable_wei: u128,
}

impl BidCurve {
    /// The share of the net a bid may use at this net.
    pub fn share_at(&self, net_usd: f64) -> f64 {
        if self.large_net_usd <= 0.0 || net_usd >= self.large_net_usd {
            return self.share_large.max(self.share);
        }
        let t = (net_usd / self.large_net_usd).clamp(0.0, 1.0);
        self.share + (self.share_large.max(self.share) - self.share) * t
    }

    /// The bid for an opportunity of `gross_usd` (before gas) and `net_usd` (after gas at the base
    /// priority) against `floor_usd`, using `gas` gas, given the competition's `ratio` (from
    /// `WinningBids::ratio_quantile`).
    pub fn priority(&self, ratio: Option<f64>, gross_usd: f64, net_usd: f64, floor_usd: f64, gas: u64, eth_usd: f64) -> Bid {
        let cap = self.max_wei.max(self.base_wei);
        if gas == 0 || eth_usd <= 0.0 {
            return Bid { priority_wei: self.base_wei, competitor_wei: None, affordable_wei: self.base_wei };
        }
        let budget_usd = ((net_usd - floor_usd).max(0.0) * self.share_at(net_usd)).max(0.0);
        let affordable = self.base_wei + (budget_usd / eth_usd * 1e18 / gas as f64) as u128;
        let competitor = ratio.map(|r| (r * gross_usd.max(0.0)) as u128);
        let target = match competitor {
            Some(c) => c + c * self.outbid_bps as u128 / 10_000 + 1,
            None => affordable,
        };
        Bid { priority_wei: target.min(affordable).clamp(self.base_wei, cap), competitor_wei: competitor, affordable_wei: affordable.min(cap) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn curve() -> BidCurve {
        BidCurve { base_wei: 1_000, max_wei: 10_000_000_000, share: 0.3, share_large: 0.9, large_net_usd: 1_000.0, quantile: 0.9, outbid_bps: 500, min_samples: 3 }
    }

    #[test]
    fn the_share_grows_with_the_size_of_the_opportunity() {
        let c = curve();
        assert!((c.share_at(0.0) - 0.3).abs() < 1e-12);
        assert!((c.share_at(500.0) - 0.6).abs() < 1e-12);
        assert!((c.share_at(5_000.0) - 0.9).abs() < 1e-12, "large: most of the profit may go");
    }

    #[test]
    fn without_observations_it_bids_the_affordable_share() {
        // net $10 above a $0 floor, share 0.3 + (0.6 x 0.01) = 0.306: $3.06 over 1M gas at $2000/ETH.
        let b = curve().priority(None, 12.0, 10.0, 0.0, 1_000_000, 2_000.0);
        assert_eq!(b.competitor_wei, None);
        assert_eq!(b.priority_wei, 1_000 + (3.06 / 2_000.0 * 1e18 / 1e6) as u128);
    }

    #[test]
    fn it_outbids_the_expected_competitor_when_the_profit_affords_it_and_never_overpays() {
        let mut w = WinningBids::new(1_000);
        // Winners paid 1e6 wei of priority per gas per USD of gross.
        for (b, p, g) in [(10, 50_000_000u128, 50.0), (20, 100_000_000, 100.0), (30, 200_000_000, 200.0)] {
            w.observe("WETH/USDC", b, p, g);
        }
        let r = w.ratio_quantile("WETH/USDC", 40, 0.9, 3).unwrap();
        assert!((r - 1e6).abs() < 1e-6);
        // Our $100 gross: a competitor would bid ~1e8; we bid 5% above, well within what $90 net affords.
        let b = curve().priority(Some(r), 100.0, 90.0, 0.0, 500_000, 2_000.0);
        assert_eq!(b.competitor_wei, Some(100_000_000));
        assert_eq!(b.priority_wei, 105_000_001, "just above the competition, not the whole affordable share");
        assert!(b.affordable_wei > b.priority_wei);
        // A thin opportunity cannot afford to win: it bids what it can, not more.
        let b = curve().priority(Some(r), 100.0, 0.001, 0.0, 500_000, 2_000.0);
        assert!(b.priority_wei < 100_000_000 && b.priority_wei == b.affordable_wei);
        // Never below the base priority.
        assert_eq!(curve().priority(Some(0.0), 100.0, 90.0, 0.0, 500_000, 2_000.0).priority_wei, 1_000);
    }

    #[test]
    fn a_quiet_market_borrows_the_others_and_old_wins_expire() {
        let mut w = WinningBids::new(100);
        for b in [10, 20, 30] {
            w.observe("busy", b, 1_000, 1.0);
        }
        w.observe("quiet", 30, 9_000, 1.0);
        assert_eq!(w.ratio_quantile("quiet", 40, 0.5, 3), Some(1_000.0), "too few of its own: all groups");
        assert_eq!(w.ratio_quantile("busy", 40, 0.5, 3), Some(1_000.0));
        assert_eq!(w.ratio_quantile("busy", 125, 0.5, 3), None, "the window moved past t = 10 and 20");
        w.observe("busy", 500, 1_000, 1.0);
        assert_eq!(w.len("busy"), 1, "pruned on insert");
    }
}
