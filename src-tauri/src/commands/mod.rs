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

// `lib.rs` calls the commands by their defining-module path
// (`commands::accounts::list_accounts`, ...), because `#[tauri::command]`
// generates its `__tauri_command_name_*` / `__cmd__*` helpers in the
// module where the function is defined — a plain `pub use` of the
// function would not carry them, and `generate_handler!` would fail
// with E0433 (PR-1.4 CI double-red, fixed here). Only the bus forwarder
// is re-exported: it is a plain function, and `lib.rs` calls it as
// `commands::spawn_bus_forwarder`.
pub use system::spawn_bus_forwarder;
