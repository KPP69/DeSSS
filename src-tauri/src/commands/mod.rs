use crate::AppState;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tauri::{AppHandle, Emitter, State};

// Frontend/backend field alignment — two rules, both load-bearing
// (incident 2026-11-26: Trading 下单 failed with
// "missing field `account_id`" because `PlaceOrderInput` lacked rule 2):
//  1. Top-level command parameters: Tauri's `#[command]` macro looks
//     arguments up by camelCase name by default, so a Rust parameter
//     `account_id` matches the frontend's `accountId` — no rename
//     needed, and no new parameter may use a name whose camelCase form
//     collides with another parameter's.
//  2. Struct fields sent over IPC: serde matches field names literally,
//     so EVERY struct that crosses the boundary needs
//     `#[serde(rename_all = "camelCase")]` — otherwise the frontend's
//     camelCase payload fails with "missing field ...". Wire DTOs live
//     in the module that owns them (`adapters::types`, `risk`,
//     `data_engine::realtime`), NOT here, so their alignment tests run
//     in the scratch crate (this file is not included there).
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AppInfo {
    pub name: String,
    pub version: String,
    pub data_dir: String,
    pub db_path: String,
    pub portable: bool,
}
#[tauri::command]
pub fn get_app_info(state: State<'_, Arc<AppState>>) -> AppInfo {
    AppInfo {
        name: "DeSSS".into(),
        version: env!("CARGO_PKG_VERSION").into(),
        data_dir: state.paths.data_dir.display().to_string(),
        db_path: state.paths.db_path.display().to_string(),
        portable: true,
    }
}
pub fn spawn_bus_forwarder(handle: AppHandle, state: Arc<AppState>) {
    tauri::async_runtime::spawn(async move {
        let mut rx = state.bus.subscribe();
        // Announce the console pipeline itself, after subscribing: if
        // this line shows up in the drawer, the whole log path works.
        // (A log published before subscribe would be lost.)
        state.bus.log(
            "info",
            "DeSSS",
            format!(
                "控制台已连接 · 代理：{}",
                crate::net::proxy::resolve_proxy().unwrap_or_else(|| "直连".to_string())
            ),
        );
        loop {
            match rx.recv().await {
                Ok(ev) => match ev {
                    crate::bus::AppEvent::Tick(t) => {
                        let _ = handle.emit("market://tick", t);
                    }
                    crate::bus::AppEvent::Balance(b) => {
                        let _ = handle.emit("account://balance", b);
                    }
                    crate::bus::AppEvent::Book(b) => {
                        let _ = handle.emit("market://book", b);
                    }
                    crate::bus::AppEvent::AccountLive(a) => {
                        let _ = handle.emit("account://live", a);
                    }
                    crate::bus::AppEvent::Log {
                        level,
                        source,
                        message,
                        ts,
                    } => {
                        let _=handle.emit("app://log", serde_json::json!({"level":level,"source":source,"message":message,"ts":ts}));
                    }
                    crate::bus::AppEvent::CatalogUpdated => {
                        let _ = handle.emit("catalog://updated", ());
                    }
                    _ => {}
                },
                // A burst (book flood) must lag the forwarder, never kill
                // it: dropping the loop here used to silence the console
                // while live data still looked fine (snapshot restore).
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    });
}

// ---- Runtime snapshot + proxy settings ----

/// Latest in-memory books + account states + recent logs, for the
/// frontend to show numbers immediately after a refresh instead of
/// waiting for the next REST poll.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeSnapshot {
    pub books: Vec<crate::adapters::types::OrderBook>,
    pub live: Vec<crate::adapters::types::AccountLive>,
    pub logs: Vec<crate::bus::LogLine>,
}

#[tauri::command]
pub fn get_runtime_snapshot(state: State<'_, Arc<AppState>>) -> RuntimeSnapshot {
    RuntimeSnapshot {
        books: state.bus.snapshot_books(),
        live: state.sync.live_states(),
        logs: state.bus.snapshot_logs(),
    }
}

/// Effective proxy: the user-configured one plus what is actually in
/// effect (custom file > env vars > Windows system proxy).
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProxyState {
    pub custom: Option<String>,
    pub effective: Option<String>,
}

#[tauri::command]
pub fn get_proxy() -> ProxyState {
    let custom = crate::net::proxy::read_proxy_file_pub();
    ProxyState {
        custom,
        effective: crate::net::proxy::resolve_proxy(),
    }
}

/// Set (or clear with an empty string) the custom local proxy, e.g.
/// `http://127.0.0.1:7897` for Clash Verge. Applies when an account is
/// (re)connected.
#[tauri::command]
pub fn set_proxy(url: String) -> Result<ProxyState, String> {
    let trimmed = url.trim();
    crate::net::proxy::write_proxy_file(if trimmed.is_empty() {
        None
    } else {
        Some(trimmed)
    })?;
    Ok(ProxyState {
        custom: crate::net::proxy::read_proxy_file_pub(),
        effective: crate::net::proxy::resolve_proxy(),
    })
}

// ---- Real read-only exchange sync (phase 3) ----
// Only the public identifier (wallet address / account index) is read from
// the vault to build an adapter. Private keys are never used here and no
// command ever returns credential material.

#[tauri::command]
pub async fn connect_account(
    state: State<'_, Arc<AppState>>,
    id: String,
) -> Result<crate::adapters::types::AccountLive, String> {
    // Connect failures used to be invisible: the frontend swallowed the
    // error and the backend logged nothing, so a failed click left the
    // account on "未连接" with no feedback at all. Log the reason to the
    // console drawer so the user can see why.
    //
    // Exception: the account was deleted while the connect was in flight.
    // That is not an error — the connect is simply cancelled — so it is
    // logged as a warning and never lights the console red dot.
    match connect_account_inner(state.inner().clone(), id).await {
        Ok(live) => Ok(live),
        Err(e) => {
            if e == "找不到该账户" || e == "该账户已被移除" {
                state
                    .bus
                    .log("warn", "DeSSS", "已移除账户，连接已取消".to_string());
            } else {
                state.bus.log("error", "DeSSS", format!("连接失败：{e}"));
            }
            Err(e)
        }
    }
}

async fn connect_account_inner(
    state: Arc<AppState>,
    id: String,
) -> Result<crate::adapters::types::AccountLive, String> {
    if state.sync.is_running(&id) {
        return Err("该账户已连接".to_string());
    }
    let row = state
        .db
        .get_account(&id)?
        .ok_or_else(|| "找不到该账户".to_string())?;
    let vault = state.vault.store()?;
    let creds = vault
        .get(&id)
        .ok_or_else(|| "该账户未保存凭证".to_string())?;
    let adapter = crate::adapters::sync::build_adapter(
        &row,
        &creds,
        state.bus.clone(),
        state.sync.live_map(),
        state.sync.reconcile_state(),
    )?;
    state.bus.log(
        "info",
        "DeSSS",
        format!("正在连接账户 {}（{}）", row.label, row.exchange_id),
    );
    adapter.connect().await.map_err(|e| e.to_string())?;
    state.sync.insert(id.clone(), adapter.clone());
    // The account may have been deleted while the connect was in flight:
    // re-check the row before spawning the run loop, or a deleted account
    // would keep syncing with no DB row behind it.
    if state.db.get_account(&id)?.is_none() {
        if let Some(adapter) = state.sync.remove(&id) {
            let _ = adapter.disconnect().await;
        }
        state.sync.remove_live(&id);
        return Err("该账户已被移除".to_string());
    }
    let a = adapter.clone();
    let handle = tauri::async_runtime::spawn(async move {
        a.run().await;
    });
    state.sync.register_task(id.clone(), handle);
    // The account may have been deleted between the DB re-check and the
    // spawn: if it is gone now, abort the just-spawned task at once
    // instead of leaving an orphaned poll loop (and its credentials)
    // behind. A delete landing after this check aborts the task via the
    // registered handle in stop_account_sync instead.
    if state.db.get_account(&id)?.is_none() {
        state.sync.abort_task(&id);
        if let Some(adapter) = state.sync.remove(&id) {
            let _ = adapter.disconnect().await;
        }
        state.sync.remove_live(&id);
        return Err("该账户已被移除".to_string());
    }
    // Return the latest live snapshot (connecting), if any was published.
    Ok(state
        .sync
        .live_states()
        .into_iter()
        .find(|l| l.account_id == id)
        .unwrap_or(crate::adapters::types::AccountLive {
            account_id: id,
            adapter_id: row.exchange_id,
            wallet_key: None,
            status: "connecting".to_string(),
            latency_ms: None,
            equity_usd: 0.0,
            available_usd: 0.0,
            pnl_usd: 0.0,
            currency: String::new(),
            positions: vec![],
            error: None,
            ts: chrono::Utc::now().timestamp_millis(),
        }))
}

/// Stop whatever is running for an account and drop its cached live
/// state: abort the run task first (so no task keeps the adapter — and
/// the credentials it holds — alive past this call), then disconnect the
/// adapter and remove the live snapshot. Used by both disconnect and
/// delete — delete must stop the sync before the DB row and vault
/// credential disappear, or the adapter would keep syncing a deleted
/// account. A disconnect failure is a warning, never a reason to block
/// the delete.
async fn stop_account_sync(state: &Arc<AppState>, id: &str) {
    state.sync.abort_task(id);
    if let Some(adapter) = state.sync.remove(id) {
        if let Err(e) = adapter.disconnect().await {
            state
                .bus
                .log("warn", "DeSSS", format!("断开账户 {id} 时出错：{e}"));
        }
    }
    state.sync.remove_live(id);
}

#[tauri::command]
pub async fn disconnect_account(state: State<'_, Arc<AppState>>, id: String) -> Result<(), String> {
    stop_account_sync(state.inner(), &id).await;
    state.bus.log("info", "DeSSS", format!("已断开账户 {id}"));
    Ok(())
}

#[tauri::command]
pub fn list_account_live(
    state: State<'_, Arc<AppState>>,
) -> Vec<crate::adapters::types::AccountLive> {
    state.sync.live_states()
}

// ---- Account center (phase 2) ----
// Credentials pass through these commands exactly once, into the encrypted
// vault. No command ever returns credential material back to the frontend.

#[tauri::command]
pub fn list_accounts(
    state: State<'_, Arc<AppState>>,
) -> Result<Vec<crate::accounts::AccountView>, String> {
    crate::accounts::list_accounts(&state.db)
}

#[tauri::command]
pub fn create_account(
    state: State<'_, Arc<AppState>>,
    label: String,
    exchange_id: String,
    credentials: serde_json::Map<String, serde_json::Value>,
) -> Result<crate::accounts::AccountView, String> {
    let vault = state.vault.store()?;
    crate::accounts::create_account(&state.db, vault, &label, &exchange_id, credentials)
}

#[tauri::command]
pub fn update_account(
    state: State<'_, Arc<AppState>>,
    id: String,
    label: String,
    exchange_id: String,
    credentials: Option<serde_json::Map<String, serde_json::Value>>,
) -> Result<crate::accounts::AccountView, String> {
    let vault = state.vault.store()?;
    crate::accounts::update_account(&state.db, vault, &id, &label, &exchange_id, credentials)
}

#[tauri::command]
pub async fn delete_account(state: State<'_, Arc<AppState>>, id: String) -> Result<(), String> {
    // Stop the sync first: abort any running connection / reconnect task
    // and drop the cached live state before the DB row and vault
    // credential disappear.
    stop_account_sync(state.inner(), &id).await;
    let vault = state.vault.store()?;
    crate::accounts::delete_account(&state.db, vault, &id)?;
    state.bus.log("info", "DeSSS", format!("已移除账户 {id}"));
    Ok(())
}

#[tauri::command]
pub fn get_vault_status(state: State<'_, Arc<AppState>>) -> Result<Option<String>, String> {
    Ok(state.vault.init_error().map(|e| e.to_string()))
}

/// Search the market catalog by base symbol (exact or prefix) or native
/// symbol (substring, case insensitive), restricted to the checked
/// exchanges. Zero-network (PR-0.3): it reads only the in-memory
/// catalog (SQLite-backed at startup) and never awaits a fetch. An
/// empty catalog answers "目录未就绪，正在拉取" and triggers one
/// background refresh; a miss on a loaded catalog triggers one
/// background refresh of the checked exchanges (60s backoff inside),
/// and the page re-searches once when `CatalogUpdated` arrives.
/// Prices come from WS frames; rows with no frame after 5s get one
/// background REST fill for the top 5 rows. An empty query returns
/// nothing — the Data page never dumps the whole market over IPC.
#[tauri::command]
pub async fn search_market_info(
    state: State<'_, Arc<AppState>>,
    query: String,
    exchange_ids: Vec<String>,
) -> Result<Vec<crate::adapters::market_catalog::MarketInfo>, String> {
    use crate::adapters::market_catalog::MarketCatalog;
    let started = Instant::now();
    if query.trim().is_empty() {
        return Ok(vec![]);
    }
    let rows = state.catalog.search(&query, &exchange_ids);
    let venues = exchange_ids.len();
    if rows.is_empty() {
        // Miss: one background refresh of the checked exchanges (the
        // gate inside skips fresh/backed-off ones); the page
        // re-searches once when `CatalogUpdated` arrives.
        let catalog = state.catalog.clone();
        let ids = exchange_ids.clone();
        tauri::async_runtime::spawn(async move {
            catalog.refresh_exchanges(&ids, false).await;
        });
        let took = started.elapsed().as_millis();
        tracing::info!("search q={query} venues={venues} hits=0 took={took}ms network=0");
        if !exchange_ids.iter().any(|id| state.catalog.has_rows_for(id)) {
            return Err("目录未就绪，正在拉取".to_string());
        }
        return Ok(vec![]);
    }
    // Rows on screen: claim the 5s-staleness fallback slot (at most one
    // task per 10s) and let WS frames arrive first.
    if state.catalog.claim_fallback_slot() {
        let catalog = state.catalog.clone();
        let streams = state.streams.clone();
        let bus = state.bus.clone();
        let snapshot = rows.clone();
        tauri::async_runtime::spawn(async move {
            tokio::time::sleep(Duration::from_secs(5)).await;
            let stale = MarketCatalog::stale_top5(&snapshot, |ex, sym| {
                streams.is_stale(&(ex.to_string(), sym.to_string()))
            });
            if !stale.is_empty() && catalog.merge_enriched(stale).await {
                bus.publish(crate::bus::AppEvent::CatalogUpdated);
            }
        });
    }
    let took = started.elapsed().as_millis();
    tracing::info!(
        "search q={query} venues={venues} hits={} took={took}ms network=0",
        rows.len()
    );
    Ok(rows)
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CatalogRefresh {
    pub count: usize,
    pub updated_at: i64,
}

/// Manually refresh the checked exchanges (ignoring the 30s cache).
/// Fire-and-forget (PR-0.3): the refresh runs in the background and
/// this returns immediately with the current snapshot; the page
/// re-searches when `CatalogUpdated` arrives. Async commands with a
/// `State` input must return a `Result` (Tauri requirement).
#[tauri::command]
pub async fn refresh_market_catalog(
    state: State<'_, Arc<AppState>>,
    exchange_ids: Vec<String>,
) -> Result<CatalogRefresh, String> {
    let catalog = state.catalog.clone();
    tauri::async_runtime::spawn(async move {
        catalog.refresh_exchanges(&exchange_ids, true).await;
    });
    Ok(CatalogRefresh {
        count: state.catalog.len(),
        updated_at: state.catalog.last_success_at().unwrap_or(0),
    })
}

/// Per-venue catalog status for the System page and the Data page
/// refresh button: row count, newest row time ("目录更新于"), and
/// the last refresh error, plus the log directory path. Sync
/// command — reads memory only.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VenueCatalogStatus {
    pub exchange_id: String,
    pub updated_at: Option<i64>,
    pub rows: usize,
    pub error: Option<String>,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CatalogStatus {
    pub venues: Vec<VenueCatalogStatus>,
    pub log_dir: String,
}

#[tauri::command]
pub fn get_catalog_status(state: State<'_, Arc<AppState>>) -> CatalogStatus {
    let venues = crate::adapters::market_catalog::KNOWN_EXCHANGES
        .iter()
        .map(|id| {
            let (rows, updated_at) = state.catalog.venue_status(id);
            VenueCatalogStatus {
                exchange_id: id.to_string(),
                updated_at,
                rows,
                error: state.catalog.last_refresh_error(id),
            }
        })
        .collect();
    CatalogStatus {
        venues,
        log_dir: state.paths.log_dir.display().to_string(),
    }
}

/// Reconcile the on-demand realtime streams with the symbols on screen.
/// The Data page calls this whenever its search hits change (and with
/// an empty list when the query is cleared or the page is left): new
/// keys start streaming, removed keys are unsubscribed at once. Sync
/// command — the engine diffs the set itself, so repeat calls with the
/// same keys are no-ops.
#[tauri::command]
pub fn set_stream_symbols(
    state: State<'_, Arc<AppState>>,
    keys: Vec<crate::data_engine::realtime::StreamKeyInput>,
) {
    let keys: Vec<(String, String)> = keys
        .into_iter()
        .map(|k| (k.exchange_id, k.symbol))
        .collect();
    state.streams.set_wanted(keys);
}

// ---- Risk gateway (phase 5A §3) ----

/// Current risk config (limits + kill switch). Sync command: the
/// gateway holds the config in memory, no DB read needed.
#[tauri::command]
pub fn get_risk_config(state: State<'_, Arc<AppState>>) -> crate::risk::RiskConfig {
    state.risk.config()
}

/// Save the risk limits. Validated first (a bad config is rejected,
/// never silently clamped), then persisted to SQLite and applied to
/// the gateway. Async commands with a `State` input must return a
/// `Result` (Tauri requirement).
#[tauri::command]
pub async fn set_risk_config(
    state: State<'_, Arc<AppState>>,
    config: crate::risk::RiskConfig,
) -> Result<crate::risk::RiskConfig, String> {
    config.save(&state.db)?;
    state.risk.set_config(config.clone());
    Ok(config)
}

/// Flip the kill switch alone (the big toggle on the Risk page).
/// Cancels are never blocked by the kill switch — it only stops new
/// orders, so the user can always exit a position.
#[tauri::command]
pub async fn set_kill_switch(
    state: State<'_, Arc<AppState>>,
    enabled: bool,
) -> Result<crate::risk::RiskConfig, String> {
    let mut config = state.risk.config();
    config.kill_switch = enabled;
    config.save(&state.db)?;
    state.risk.set_config(config.clone());
    state.bus.log(
        "warn",
        "risk",
        if enabled {
            "风控总开关已开启：禁止一切新下单（撤单不受影响）".to_string()
        } else {
            "风控总开关已关闭：恢复下单".to_string()
        },
    );
    Ok(config)
}

/// Drawdown snapshot for the Risk page: session initial equity,
/// high-water mark, latest sampled equity and current drawdown.
/// Sync command: the gateway holds the numbers in memory.
#[tauri::command]
pub fn get_drawdown_status(state: State<'_, Arc<AppState>>) -> crate::risk::DrawdownStatus {
    state.risk.drawdown_status()
}

// ---------------------------------------------------------------------------
// Orders (phase 5A §4) — every write goes through the execution
// engine (intent → risk → execution → adapter). The commands below
// are thin wrappers: they build the deps and hand over.
// ---------------------------------------------------------------------------

fn exec_deps(state: &State<'_, Arc<AppState>>) -> crate::execution::ExecDeps {
    crate::execution::ExecDeps {
        risk: state.risk.clone(),
        orders: state.orders.clone(),
        sync: state.sync.clone(),
        streams: state.streams.clone(),
        catalog: state.catalog.clone(),
        bus: state.bus.clone(),
    }
}

/// Place one order. Returns the order record — Open / Filled /
/// Unresolved are all Ok (Unresolved is a result the user must see,
/// with the reason in `error`); definite failures are Err.
#[tauri::command]
pub async fn place_order(
    state: State<'_, Arc<AppState>>,
    input: crate::adapters::types::PlaceOrderInput,
) -> Result<crate::adapters::types::OrderInfo, String> {
    let deps = exec_deps(&state);
    crate::execution::place_order(
        &deps,
        crate::execution::PlaceIntent {
            account_id: input.account_id,
            // No case rewrite here either: the execution layer looks
            // the symbol up case-insensitively and uses the catalog
            // row's symbol verbatim downstream (rule 2.2).
            symbol: input.symbol.trim().to_string(),
            side: input.side,
            order_type: input.order_type,
            quantity: input.quantity,
            price: input.price,
        },
    )
    .await
}

/// Cancel one order and wait for the result. `order_id` is the
/// exchange oid (from the open-orders list) or a cloid.
#[tauri::command]
pub async fn cancel_order(
    state: State<'_, Arc<AppState>>,
    account_id: String,
    order_id: String,
    symbol: String,
) -> Result<crate::adapters::types::OrderInfo, String> {
    let deps = exec_deps(&state);
    crate::execution::cancel_order(&deps, &account_id, &order_id, &symbol).await
}

/// Live open orders from the exchange, merged with our records.
#[tauri::command]
pub async fn list_open_orders(
    state: State<'_, Arc<AppState>>,
    account_id: String,
) -> Result<Vec<crate::adapters::types::OrderInfo>, String> {
    let deps = exec_deps(&state);
    crate::execution::list_open_orders(&deps, &account_id).await
}

/// Order records kept by the execution engine (newest first).
/// Sync command: the store is in-memory.
#[tauri::command]
pub fn list_order_records(
    state: State<'_, Arc<AppState>>,
) -> Vec<crate::adapters::types::OrderInfo> {
    state.orders.list()
}

/// Recorded fills (newest first). Sync command.
#[tauri::command]
pub fn list_order_fills(state: State<'_, Arc<AppState>>) -> Vec<crate::adapters::types::TradeInfo> {
    state.orders.list_fills()
}
