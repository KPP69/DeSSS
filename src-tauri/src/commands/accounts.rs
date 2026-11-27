use crate::AppState;
use std::sync::Arc;
use tauri::State;

// ---- Real read-only exchange sync (phase 3) ----
// Only the public identifier (wallet address / account index) is read from
// the vault to build an adapter. Private keys are never used here and no
// command ever returns credential material.

#[tauri::command]
pub async fn connect_account(
    state: State<'_, Arc<AppState>>,
    id: String,
) -> Result<crate::core::types::AccountLive, String> {
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
) -> Result<crate::core::types::AccountLive, String> {
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
    let adapter = crate::market::sync::build_adapter(
        &row,
        &creds,
        state.bus.clone(),
        state.sync.live_map(),
        state.sync.reconcile_state(),
        state.orders.clone(),
        state.sync.clone(),
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
        .unwrap_or(crate::core::types::AccountLive {
            account_id: id,
            adapter_id: row.exchange_id,
            wallet_key: None,
            status: "connecting".to_string(),
            latency_ms: None,
            equity_usd: 0.0,
            available_usd: 0.0,
            pnl_usd: None,
            currency: String::new(),
            positions: vec![],
            source: "rest".to_string(),
            degraded: false,
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
pub fn list_account_live(state: State<'_, Arc<AppState>>) -> Vec<crate::core::types::AccountLive> {
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
