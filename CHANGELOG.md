# Changelog

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
