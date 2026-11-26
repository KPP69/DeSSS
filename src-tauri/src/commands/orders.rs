use crate::AppState;
use std::sync::Arc;
use tauri::State;

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
    input: crate::core::types::PlaceOrderInput,
) -> Result<crate::core::types::OrderInfo, String> {
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
) -> Result<crate::core::types::OrderInfo, String> {
    let deps = exec_deps(&state);
    crate::execution::cancel_order(&deps, &account_id, &order_id, &symbol).await
}

/// Live open orders from the exchange, merged with our records.
#[tauri::command]
pub async fn list_open_orders(
    state: State<'_, Arc<AppState>>,
    account_id: String,
) -> Result<Vec<crate::core::types::OrderInfo>, String> {
    let deps = exec_deps(&state);
    crate::execution::list_open_orders(&deps, &account_id).await
}

/// Order records kept by the execution engine (newest first).
/// Sync command: the store is in-memory.
#[tauri::command]
pub fn list_order_records(state: State<'_, Arc<AppState>>) -> Vec<crate::core::types::OrderInfo> {
    state.orders.list()
}

/// Recorded fills (newest first). Sync command.
#[tauri::command]
pub fn list_order_fills(state: State<'_, Arc<AppState>>) -> Vec<crate::core::types::TradeInfo> {
    state.orders.list_fills()
}
