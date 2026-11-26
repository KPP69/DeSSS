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
//     in the module that owns them (`core::types`, `risk`,
//     `data_engine::realtime`), NOT here, so their alignment tests run
//     in the scratch crate (this file is not included there).
pub mod accounts;
pub mod market;
pub mod orders;
pub mod risk;
pub mod strategy;
pub mod system;

// Re-exported so `commands::<name>` keeps working from lib.rs
// (the handler and the bus forwarder use these paths).
pub use accounts::{
    connect_account, create_account, delete_account, disconnect_account, get_vault_status,
    list_account_live, list_accounts, update_account,
};
pub use market::{
    get_catalog_status, refresh_market_catalog, search_market_info, set_stream_symbols,
};
pub use orders::{
    cancel_order, list_open_orders, list_order_fills, list_order_records, place_order,
};
pub use risk::{get_drawdown_status, get_risk_config, set_kill_switch, set_risk_config};
pub use system::{get_app_info, get_proxy, get_runtime_snapshot, set_proxy, spawn_bus_forwarder};
