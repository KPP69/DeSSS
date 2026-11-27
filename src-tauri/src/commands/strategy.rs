//! Strategy commands (Phase 3.1, commit 1): the Strategy page's
//! surface over the `strategy` module. Thin wrappers — validation
//! and storage live in `crate::strategy` so the scratch crate tests
//! them; the commands only adapt Tauri state to those functions.
//! Command names are the function names: `list_strategies`,
//! `save_strategy`, `set_strategy_enabled`.

use crate::strategy::{SaveStrategyInput, SaveStrategyResult, StrategyInstance, StrategyView};
use crate::AppState;
use std::sync::Arc;
use tauri::State;

/// All persisted strategy instances with their runtime stats (the
/// page refreshes this after every save / enable toggle).
#[tauri::command]
pub fn list_strategies(state: State<'_, Arc<AppState>>) -> Result<Vec<StrategyView>, String> {
    crate::strategy::list_strategy_views(&state.db, &state.orders, &state.strategy_stats)
}

/// Create or replace one instance. Returns the stored instance plus
/// the midline warning when it applies (a warning, not an error —
/// the save succeeds).
#[tauri::command]
pub fn save_strategy(
    state: State<'_, Arc<AppState>>,
    input: SaveStrategyInput,
) -> Result<SaveStrategyResult, String> {
    crate::strategy::save_strategy(&state.db, &state.sync, input)
}

/// Flip one instance's enabled flag. The runner picks the change up
/// within one 200ms round.
#[tauri::command]
pub fn set_strategy_enabled(
    state: State<'_, Arc<AppState>>,
    id: String,
    enabled: bool,
) -> Result<StrategyInstance, String> {
    crate::strategy::set_strategy_enabled(&state.db, &id, enabled)
}
