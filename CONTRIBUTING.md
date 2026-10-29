# Contributing

[繁體中文](CONTRIBUTING.zh-TW.md)

Thanks for helping. Code in this crate signs and sends transactions for every bot built on it, so
correctness and safety come before features.

## Checks every pull request must pass

```
cargo check --all-targets   # 0 warnings
cargo clippy --all-targets   # 0 warnings
cargo test
cargo test -- --ignored     # needs anvil: the executor task on a local chain
```

A change here is also checked against both frameworks
([Debt-keeper](https://github.com/foodpenguin/Debt-keeper),
[Arb-seeker](https://github.com/foodpenguin/Arb-seeker)): build them against your checkout (see
"Using it" in the README) and run their tests (and their anvil e2e tests when the executor, the gates or the
simulation change).

## Guidelines

- **Nothing protocol- or strategy-specific** belongs here. If only one bot needs it, it lives in
  that bot's framework.
- **The shared nonce file format** is a protocol between processes: change it only with a new
  version tag (`SHARED_NONCE_VERSION`), never in place.
- **Keep the hot path fast**: no extra RPC calls on the send path, no blocking work in the executor
  task.
- **Comments in English**, matching the surrounding style. Docs come in English and Traditional
  Chinese (`*.zh-TW.md`): update both, or say in the PR that a translation is pending.
- **No secrets** in commits: no keys, no RPC URLs with API keys, no personal wallet addresses.
- Never send mainnet transactions from tests or tooling.

## License

By contributing you agree that your contribution is licensed under the
[GNU AGPL v3.0 or later](LICENSE), like the rest of the project.
