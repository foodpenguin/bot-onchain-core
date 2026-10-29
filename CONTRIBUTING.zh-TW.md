# 貢獻指南

[English](CONTRIBUTING.md)

感謝你的幫忙。建立在這個 crate 上的每個 bot，都由這裡的程式碼簽名和送出交易，所以正確性和安全
優先於新功能。

## 每個 pull request 都要通過的檢查

```
cargo check --all-targets   # 0 warnings
cargo clippy --all-targets   # 0 warnings
cargo test
cargo test -- --ignored     # 需要 anvil：在本地鏈上測試 executor task
```

這裡的改動也要在兩個框架（[Debt-keeper](https://github.com/foodpenguin/Debt-keeper)、
[Arb-seeker](https://github.com/foodpenguin/Arb-seeker)）上檢查：用你本機的 checkout 編譯它們（見 README
的「使用方式」）並跑它們的測試（改到 executor、關卡或模擬時，還要跑它們的 anvil e2e 測試）。

## 原則

- **任何協議或策略專屬的東西**都不屬於這裡。如果只有一個 bot 需要，就放在那個 bot 的框架裡。
- **共用 nonce 檔的格式**是 process 之間的協定：只能搭配新的版本標記（`SHARED_NONCE_VERSION`）
  修改，絕不能原地改。
- **熱路徑要快**：送單路徑上不加額外的 RPC 呼叫，executor task 裡不做會阻塞的工作。
- **註解用英文**，風格跟周圍的程式一致。文件有英文和繁體中文（`*.zh-TW.md`）兩版：兩版都要更新，
  或在 PR 裡註明翻譯待補。
- commit 裡**不能有機密**：不放私鑰、含 API key 的 RPC URL、個人錢包地址。
- 測試和工具絕不送主網交易。

## 授權

提交貢獻即表示你同意你的貢獻和本專案其他部分一樣，採用
[GNU AGPL v3.0 或更新版本](LICENSE)授權。
