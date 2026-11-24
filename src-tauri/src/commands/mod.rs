use tauri::{State, AppHandle, Emitter}; use std::sync::Arc; use crate::AppState;
#[derive(serde::Serialize)] #[serde(rename_all="camelCase")] pub struct AppInfo{ pub name:String, pub version:String, pub data_dir:String, pub db_path:String, pub portable:bool }
#[tauri::command] pub fn get_app_info(state: State<'_, Arc<AppState>>)->AppInfo{ AppInfo{name:"DeSSS".into(),version:env!("CARGO_PKG_VERSION").into(),data_dir:state.paths.data_dir.display().to_string(),db_path:state.paths.db_path.display().to_string(),portable:true} }
#[tauri::command] pub async fn start_mock(state: State<'_, Arc<AppState>>)->Result<(),String>{ state.mock.clone().start_loops(); Ok(()) }
#[tauri::command] pub fn stop_mock(state: State<'_, Arc<AppState>>)->Result<(),String>{ state.mock.stop(); Ok(()) }
#[tauri::command] pub async fn get_snapshot(state: State<'_, Arc<AppState>>)->Result<serde_json::Value,String>{ use crate::adapters::ExchangeAdapter; let b=state.mock.get_balances("mock-account").await.map_err(|e|e.to_string())?; Ok(serde_json::json!({"ticks":[],"balances":b})) }
pub fn spawn_bus_forwarder(handle: AppHandle, state: Arc<AppState>) { tauri::async_runtime::spawn(async move { let mut rx=state.bus.subscribe(); while let Ok(ev)=rx.recv().await { match ev { crate::bus::AppEvent::Tick(t)=>{ let _=handle.emit("market://tick", t); }, crate::bus::AppEvent::Balance(b)=>{ let _=handle.emit("account://balance", b); }, crate::bus::AppEvent::Book(b)=>{ let _=handle.emit("market://book", b); }, crate::bus::AppEvent::AccountLive(a)=>{ let _=handle.emit("account://live", a); }, _=>{} } } }); }

// ---- Real read-only exchange sync (phase 3) ----
// Only the public identifier (wallet address / account index) is read from
// the vault to build an adapter. Private keys are never used here and no
// command ever returns credential material.

#[tauri::command]
pub async fn connect_account(
    state: State<'_, Arc<AppState>>,
    id: String,
) -> Result<crate::adapters::types::AccountLive, String> {
    if state.sync.is_running(&id) {
        return Err("account is already connected".to_string());
    }
    let row = state.db.get_account(&id)?
        .ok_or_else(|| "account not found".to_string())?;
    let vault = state.vault.store()?;
    let creds = vault
        .get(&id)
        .ok_or_else(|| "no credentials stored for this account".to_string())?;
    let adapter = crate::adapters::sync::build_adapter(&row, &creds, state.bus.clone(), state.sync.live_map())?;
    adapter.connect().await.map_err(|e| e.to_string())?;
    state.sync.insert(id.clone(), adapter.clone());
    let a = adapter.clone();
    tauri::async_runtime::spawn(async move {
        use crate::adapters::ExchangeAdapter;
        a.run().await;
    });
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

#[tauri::command]
pub async fn disconnect_account(state: State<'_, Arc<AppState>>, id: String) -> Result<(), String> {
    if let Some(adapter) = state.sync.remove(&id) {
        adapter.disconnect().await.map_err(|e| e.to_string())?;
    }
    state.sync.remove_live(&id);
    Ok(())
}

#[tauri::command]
pub fn list_account_live(state: State<'_, Arc<AppState>>) -> Vec<crate::adapters::types::AccountLive> {
    state.sync.live_states()
}

// ---- Account center (phase 2) ----
// Credentials pass through these commands exactly once, into the encrypted
// vault. No command ever returns credential material back to the frontend.

#[tauri::command]
pub fn list_accounts(state: State<'_, Arc<AppState>>) -> Result<Vec<crate::accounts::AccountView>, String> {
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
pub fn delete_account(state: State<'_, Arc<AppState>>, id: String) -> Result<(), String> {
    let vault = state.vault.store()?;
    crate::accounts::delete_account(&state.db, vault, &id)
}

#[tauri::command]
pub fn get_vault_status(state: State<'_, Arc<AppState>>) -> Result<Option<String>, String> {
    Ok(state.vault.init_error().map(|e| e.to_string()))
}
