use crate::AppState;
use std::sync::Arc;
use tauri::State;

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
