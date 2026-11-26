pub mod accounts;
pub mod adapters;
pub mod bus;
pub mod commands;
pub mod core;
pub mod data_engine;
pub mod execution;
pub mod logging;
pub mod net;
pub mod portable;
pub mod risk;
pub mod strategy_engine;
pub mod vault;
pub mod venues;

use std::sync::Arc;
use std::time::Duration;

use adapters::market_catalog::MarketCatalog;
use adapters::sync::SyncManager;
use bus::EventBus;
use execution::OrderStore;
use portable::PortablePaths;
use risk::{RiskConfig, RiskGateway};

pub struct AppState {
    pub paths: PortablePaths,
    pub bus: EventBus,
    pub db: Arc<data_engine::DataEngine>,
    pub vault: Arc<vault::VaultHandle>,
    pub sync: Arc<SyncManager>,
    pub catalog: Arc<MarketCatalog>,
    pub streams: Arc<data_engine::realtime::RealtimeEngine>,
    pub risk: Arc<RiskGateway>,
    /// In-memory order records + fills (the execution engine owns
    /// it; the exchange is the source of truth).
    pub orders: Arc<OrderStore>,
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    // First thing: route every panic to ./data/logs/crash.log (portable dir),
    // so failures during init or in setup() are never silent.
    portable::install_panic_hook();

    let paths = PortablePaths::resolve();
    paths.ensure_dirs().expect("create ./data");
    // File logging first (PR-0.3): ./data/logs/app.log, 5 MB x 2
    // rolling. Best-effort — a failure only drops log lines.
    logging::init(&paths.log_dir);
    let db = Arc::new(data_engine::DataEngine::open(paths.clone()).expect("open desss.db"));
    // The vault never aborts startup: if vault.enc is corrupted or vault.key
    // is missing, the handle keeps the error and account commands surface it,
    // while the original files stay untouched.
    let vault = Arc::new(vault::VaultHandle::open(&paths));
    if let Some(err) = vault.init_error() {
        eprintln!("[vault] running with vault unavailable: {err}");
    }
    // 4096: the bus carries high-frequency book frames; a burst must lag
    // the forwarder, never kill it (see spawn_bus_forwarder).
    let bus = EventBus::new(4096);
    let sync = Arc::new(SyncManager::new());
    let catalog = Arc::new(MarketCatalog::with_db(bus.clone(), db.clone()));
    let streams = Arc::new(data_engine::realtime::RealtimeEngine::new(
        bus.clone(),
        catalog.clone(),
        Some(sync.clone()),
    ));
    // Risk config loads from SQLite `settings` (a kill switch left on
    // stays on across restarts); the gateway is the single source of
    // truth while the app runs.
    let risk = Arc::new(RiskGateway::new(RiskConfig::load(&db)));
    let orders = Arc::new(OrderStore::new());
    let state = Arc::new(AppState {
        paths,
        bus,
        db,
        vault,
        sync,
        catalog,
        streams,
        risk,
        orders,
    });
    let s2 = state.clone();
    let sampler_state = state.clone();
    let sync_state = state.clone();
    let catalog_state = state.clone();
    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .manage(state)
        .setup(move |app| {
            // Drawdown sampler (spec §3 §4): every 10s, record total equity
            // across connected accounts so the session high-water mark tracks
            // the live state, not just order attempts. The order path also
            // records a fresh sample right before each risk check.
            // Spawned here — NOT in `run()` — via `tauri::async_runtime::spawn`:
            // `run()` executes on a plain sync thread with no Tokio context,
            // so `tokio::spawn` there panics ("there is no reactor running",
            // real-machine crash 2026-11-26, lib.rs:85). `setup` runs with the
            // app runtime up, and `tauri::async_runtime::spawn` works from
            // any thread. Same rule everywhere: no production `tokio::spawn`.
            {
                let state = sampler_state.clone();
                tauri::async_runtime::spawn(async move {
                    let mut tick = tokio::time::interval(Duration::from_secs(10));
                    loop {
                        tick.tick().await;
                        let total: f64 = state.sync.total_equity();
                        if total > 0.0 {
                            state.risk.record_equity(total);
                        }
                    }
                });
            }
            // Open-orders sync (phase 5A §4 follow-up, real-machine
            // 2026-11-26): every 10s, reconcile each connected
            // Hyperliquid / Entropy account's open orders with the
            // exchange (see `execution::sync_open_orders`). The
            // exchange's live list is the only source of truth:
            // orders canceled on the website disappear from the
            // store, website-placed orders appear and become
            // cancellable. Best-effort — a failure is retried next
            // cycle, and the account poll loop already surfaces REST
            // failures, so this task logs nothing itself. Same spawn
            // rule as the sampler above: `tauri::async_runtime::spawn`
            // inside `setup`, never `tokio::spawn`.
            {
                let state = sync_state.clone();
                tauri::async_runtime::spawn(async move {
                    let mut tick = tokio::time::interval(Duration::from_secs(10));
                    loop {
                        tick.tick().await;
                        let deps = crate::execution::ExecDeps {
                            risk: state.risk.clone(),
                            orders: state.orders.clone(),
                            sync: state.sync.clone(),
                            streams: state.streams.clone(),
                            catalog: state.catalog.clone(),
                            bus: state.bus.clone(),
                        };
                        for (account_id, exchange_id) in state.sync.connected_accounts() {
                            if exchange_id != "hyperliquid" && exchange_id != "entropy" {
                                continue;
                            }
                            let _ = crate::execution::sync_open_orders(&deps, &account_id).await;
                        }
                    }
                });
            }
            // Portable WebView2 cache: the main window is created here (not
            // from the static `windows` array in tauri.conf.json) so its
            // user-data directory can be forced to exe-adjacent
            // ./data/webview, leaving no browser cache on C:.
            tauri::WebviewWindowBuilder::new(
                app,
                "main",
                tauri::WebviewUrl::App("index.html".into()),
            )
            .title("DeSSS")
            .inner_size(1280.0, 800.0)
            .min_inner_size(960.0, 640.0)
            .data_directory(portable::data_dir().join("webview"))
            .build()?;
            let handle = app.handle().clone();
            commands::spawn_bus_forwarder(handle, s2.clone());
            // Market catalog background refresh (PR-0.3): one async
            // refresh at startup, then every 12h. Search never waits
            // for it — the catalog is SQLite-backed, and the page
            // re-searches once per `CatalogUpdated` event. Same spawn
            // rule as the sampler above.
            {
                let state = catalog_state.clone();
                tauri::async_runtime::spawn(async move {
                    let ids: Vec<String> = crate::venues::all()
                        .iter()
                        .map(|v| v.id().to_string())
                        .collect();
                    state.catalog.refresh_exchanges(&ids, false).await;
                    loop {
                        tokio::time::sleep(Duration::from_secs(12 * 3600)).await;
                        state.catalog.refresh_exchanges(&ids, false).await;
                    }
                });
            }
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            commands::get_app_info,
            commands::list_accounts,
            commands::create_account,
            commands::update_account,
            commands::delete_account,
            commands::get_vault_status,
            commands::connect_account,
            commands::disconnect_account,
            commands::list_account_live,
            commands::get_runtime_snapshot,
            commands::get_proxy,
            commands::set_proxy,
            commands::search_market_info,
            commands::refresh_market_catalog,
            commands::get_catalog_status,
            commands::set_stream_symbols,
            commands::get_risk_config,
            commands::set_risk_config,
            commands::set_kill_switch,
            commands::get_drawdown_status,
            commands::place_order,
            commands::cancel_order,
            commands::list_open_orders,
            commands::list_order_records,
            commands::list_order_fills
        ])
        .run(tauri::generate_context!())
        .expect("error while running DeSSS");
}
