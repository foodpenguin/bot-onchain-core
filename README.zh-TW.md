# bot-onchain-core

[English](README.md)

鏈上 bot 共用的基礎管線，是一個 Rust 函式庫（[alloy](https://github.com/alloy-rs/alloy)）：
RPC 池、新區塊和 Flashblocks 串流、用 `eth_simulateV1` 做的精確模擬、keystore 簽名、廣播、
bot 之間的 nonce 協調、executor（作為獨立的 task）、送單關卡，以及依觀察到的競爭對手決定的
priority fee 出價。crate 名稱是 `onchain-core`；[Debt-keeper](https://github.com/foodpenguin/Debt-keeper)（清算）
和 [Arb-seeker](https://github.com/foodpenguin/Arb-seeker)（套利）都建立在它之上。它對借貸協議或套利策略
一無所知。

> **狀態：**實驗性。這裡沒有任何投資建議。會簽名和送單的程式碼是在本地 anvil 鏈上測試的；請先用
> shadow／觀察模式跑你的 bot。

## 模組

| 模組 | 職責 |
|---|---|
| `rpc` | 給 alloy 用的多端點 JSON-RPC 池：依優先順序競速或故障轉移、每個端點的 `skip_methods` 和 log 範圍限制、落後的節點改用其他端點但不進入冷卻、容量錯誤（以片語判斷）會故障轉移，呼叫本身的確定性錯誤絕不會。log 只顯示端點名稱，絕不顯示 URL（URL 含有 API key）。 |
| `heads` | 新區塊訊號：同時在多個端點上用 WS `newHeads`（先到的勝出），HTTP 輪詢備援。 |
| `flashblocks` | Base Flashblocks：訊息解碼、會自動重連的讀取器（`run_stream`），以及依到達順序取得每一個 flashblock（`spawn_raw`）。其他模組都不依賴它（Base 的 "Denim" 升級會移除 Flashblocks）。 |
| `chain`、`num` | Multicall3 批次讀取；單位換算和全精度的乘除。 |
| `sim` | 把一個呼叫照它實際會執行的樣子精確模擬：在某個區塊加上 state override 上執行 `eth_simulateV1`，可選擇當成下一個區塊，以實際的 fee 計價（發送者的餘額覆寫讓節點不會拒絕），回傳用掉的 gas、L1 data fee，以及在同一個狀態裡的可選探測讀取。 |
| `executor` | 從加密 keystore 簽名（密碼來自讀完就刪除的環境變數，或提示輸入）、透過預熱的連線廣播到每個端點、本地 nonce 加上每個錢包一份的共用 nonce 檔、多筆 nonce 連續的交易同時在飛（`TxKey` = 這筆交易針對的對象；key 相同的兩筆絕不同時在飛；`exclusive_group` 的送單在同一 group 已有交易在飛時由 executor 自己拒絕；這些拒絕是有型別的 `SendRefused`，呼叫端分得出是搶輸了還是真的失敗）、被丟棄或卡住的交易用 0 元自轉帳取代、失敗冷卻。 |
| `actor` | 作為獨立 task 的 executor：`ExecHandle` 同步讀取它的狀態，並以訊息送單；每個公告的區塊都會輪詢 receipt；已結算的交易透過 channel 回傳。規劃從不等 receipt；區塊迴圈可以用 `wait_polled` 短暫等新區塊的 receipt 查完（最多 `RECEIPT_WAIT`），讓剛上鏈的交易不再算在飛。 |
| `gates` | 每次送單前的檢查：緊急停止檔案、revert 交易燒掉的 gas 的每日預算（在飛的交易預留它們的最壞成本直到結算：`reserve` / `release`，同時送出的多筆不會一起超過上限；已有預留的 key 會被拒絕，絕不覆蓋），以及以區塊計的期限、gas 和 L1 fee 成本（OP-stack GasPriceOracle）。 |
| `bid` | 對抗競爭的 priority fee：`WinningBids` 記錄過去的得標者每 1 USD 毛利付了多少（依群組、在一段時間窗內）；`BidCurve` 出價略高於預測的競爭對手，但絕不超過淨利的一定比例（比例隨機會的規模增加），也絕不低於基本 priority。 |
| `config` | 每個 bot 都會 `#[serde(flatten)]` 進自己設定的區段：`RpcSettings`（端點和存放它們 key 的 `.env`）以及 `SendSettings`（錢包、模式、keystore、廣播、緊急停止開關、nonce 檔、預算、失敗處理、`max_inflight`）。 |

## 使用方式

依賴帶 tag 的版本（兩個框架也是這樣鎖定版本）：

```toml
[dependencies]
onchain-core = { git = "https://github.com/foodpenguin/bot-onchain-core", tag = "v0.1.0" }
```

要讓框架改用本機 checkout 的這個 repo 編譯（例如兩邊同時在改），在框架的 `bot/` 目錄覆寫依賴：

```
cargo test --config 'patch."https://github.com/foodpenguin/bot-onchain-core".onchain-core.path="../../bot-onchain-core"'
```

最小的送單範例：

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

## 共用的 nonce 檔

替同一個錢包簽名的每個 bot，在簽名加廣播的期間都會鎖住每個錢包一份的檔案（預設
`~/.local/state/onchain-bots/nonce-<wallet>`），並把下一個 nonce 以 `v1 <next nonce> <unix ms>`
的格式記在裡面。遇到不認得的格式時，bot 會拒絕送單而不是用猜的（`executor::SHARED_NONCE_VERSION`）。
唯一的例外是先前版本留下的無版本格式（`<next nonce> <unix ms>`）：已過期的（超過 30 秒）視為空的並覆寫，
所以升級後不必手動刪檔；仍然新鮮的代表舊版 bot 可能正在送單，一樣拒絕。
建立在不同且互不相容版本上的 bot，不可以共用同一個錢包。

## 檢查

```
cargo check --all-targets   # 0 warnings
cargo clippy --all-targets   # 0 warnings
cargo test                  # 單元測試，包含 nonce 鎖的雙 process 測試
cargo test -- --ignored     # 在本地 anvil 鏈上測試 executor task（需要 anvil）
```

## 授權

採用 [GNU Affero General Public License v3.0 或更新版本](LICENSE)授權。本軟體不提供任何擔保。
回報漏洞請見 [SECURITY.md](SECURITY.md)，貢獻方式請見 [CONTRIBUTING.zh-TW.md](CONTRIBUTING.zh-TW.md)。
