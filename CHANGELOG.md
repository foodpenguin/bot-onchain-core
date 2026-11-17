# Changelog

## v0.1.5 (unreleased)

Compatible with v0.1.4: no renames, no removed items.

- `Executor::send_with_value` / `ExecHandle::send_with_value`: a send with ETH attached (e.g. moving
  ETH the wallet received on to another address), with the same checks, nonce handling and in-flight
  tracking as `send` (which is now `send_with_value` with zero value).
- `Inflight.value`: the ETH a tx in flight carries (serialized only when non-zero), reserved by
  `ExecView::spendable` / `ExecHandle::spendable` along with its worst-case gas until it settles.
  `Inflight` is only built by the executor, so code that reads it is unaffected.

## v0.1.4 (2026-11-17)

Compatible with v0.1.3: no renames, no removed items.

- `RpcPool::with_eager_hedge(methods, max_in_flight)`: these JSON-RPC methods are hedged to a second
  same-tier endpoint immediately (first answer wins) instead of after ~2x the first endpoint's
  latency, for a latency-critical call worth racing a duplicate — e.g. the Atlas valuation's
  `eth_simulateV1`, where the first endpoint often lags a block. At most `max_in_flight` such
  requests are eagerly hedged at once (0 = off); past it they hedge after the usual delay, so a
  burst does not double the pool's load when it is busiest.

## v0.1.3 (2026-11-09)

Compatible with v0.1.2: no renames, no removed items.

- `sim::simulate_call`: a revert reports the call's `error.data` when `returnData` is empty. Geth-style
  `eth_simulateV1` returns `returnData: "0x"` on a revert and carries the revert data in
  `error.data`, so `SimReverted` used to read just "execution reverted 0x".

## v0.1.2 (2026-11-06)

Compatible with v0.1.1: no renames, no removed items.

- `heads::spawn_ws_many`: the WebSocket reconnect loop backs off exponentially (1 s to 60 s) and
  parks an endpoint that fails more than 20 times in a rolling hour for 5 minutes, logging one WARN
  instead of thousands. A connection that stayed up over a minute resets the backoff and the
  failure window. This stops a dead free-tier socket (seen after Base Denim) from spinning a
  reconnect/error storm.

## v0.1.1 (2026-11-02)

Compatible with v0.1.0: no renames, no removed items.

- `executor::account_nonce`: reads the "pending" nonce, and the latest one from then on once a
  node refuses the "pending" tag (Base Denim drops pending state). `Executor::sync_nonce` and
  `spawn_nonce_watch` use it. `cancel_stuck` still compares latest and pending: where the two
  are equal it never fires, so keep `max_inflight = 1` on such a chain.
- `gates::DailyBudget` derives `Serialize`/`Deserialize` (the day and its spend; reservations are
  not serialized), so a bot can persist its failed-gas spend across restarts.
- `rpc::RpcPool::method_bytes_report`: request bytes sent per endpoint and method (egress
  accounting). WebSocket subscriptions and raw-tx broadcasts are not counted.

## v0.1.0 (2026-10-29)

First release.
