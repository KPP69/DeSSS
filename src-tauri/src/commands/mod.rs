use crate::AppState;
use std::sync::Arc;
use tauri::{AppHandle, Emitter, State};
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
    let handle = tokio::spawn(async move {
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
/// exchanges. A search fetches a checked exchange's bulk endpoint only
/// when its cache is older than 30s; otherwise the cache is served. An
/// empty query returns nothing — the Data page never dumps the whole
/// market over IPC. Public data needs no account and never touches
/// account credentials. Matched rows then get a short-cycle (2s) live
/// refresh of mark price + funding, and are enriched with the true
/// per-symbol BBO (and AsterDEX open interest), which the bulk endpoints
/// cannot provide.
#[tauri::command]
pub async fn search_market_info(
    state: State<'_, Arc<AppState>>,
    query: String,
    exchange_ids: Vec<String>,
) -> Result<Vec<crate::adapters::market_catalog::MarketInfo>, String> {
    state.catalog.refresh_exchanges(&exchange_ids, false).await;
    let mut rows = state.catalog.search(&query, &exchange_ids);
    state.catalog.refresh_live_rows(&mut rows).await;
    Ok(state.catalog.enrich_rows(rows).await)
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CatalogRefresh {
    pub count: usize,
    pub updated_at: i64,
}

/// Manually refresh the checked exchanges (ignoring the 30s cache) and
/// return the cached row count plus the last *successful* refresh
/// timestamp (unix ms) for the Data page's "缓存更新于" display — a cycle
/// where every exchange failed leaves the timestamp untouched. Async
/// commands with a `State` input must return a `Result` (Tauri requirement).
#[tauri::command]
pub async fn refresh_market_catalog(
    state: State<'_, Arc<AppState>>,
    exchange_ids: Vec<String>,
) -> Result<CatalogRefresh, String> {
    state.catalog.refresh_exchanges(&exchange_ids, true).await;
    Ok(CatalogRefresh {
        count: state.catalog.len(),
        updated_at: state.catalog.last_success_at().unwrap_or(0),
    })
}
