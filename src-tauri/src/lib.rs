pub mod accounts;
pub mod adapters;
pub mod bus;
pub mod commands;
pub mod data_engine;
pub mod portable;
pub mod risk;
pub mod strategy_engine;
pub mod vault;

use std::sync::Arc;

use adapters::mock::MockAdapter;
use adapters::sync::SyncManager;
use bus::EventBus;
use portable::PortablePaths;

pub struct AppState {
    pub paths: PortablePaths,
    pub bus: EventBus,
    pub mock: Arc<MockAdapter>,
    pub db: data_engine::DataEngine,
    pub vault: Arc<vault::VaultHandle>,
    pub sync: Arc<SyncManager>,
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    // First thing: route every panic to ./data/logs/crash.log (portable dir),
    // so failures during init or in setup() are never silent.
    portable::install_panic_hook();

    let paths = PortablePaths::resolve();
    paths.ensure_dirs().expect("create ./data");
    let db = data_engine::DataEngine::open(paths.clone()).expect("open desss.db");
    // The vault never aborts startup: if vault.enc is corrupted or vault.key
    // is missing, the handle keeps the error and account commands surface it,
    // while the original files stay untouched.
    let vault = Arc::new(vault::VaultHandle::open(&paths));
    if let Some(err) = vault.init_error() {
        eprintln!("[vault] running with vault unavailable: {err}");
    }
    let bus = EventBus::new(1024);
    let mock = Arc::new(MockAdapter::new(bus.clone()));
    let sync = Arc::new(SyncManager::new());
    let state = Arc::new(AppState {
        paths,
        bus,
        mock,
        db,
        vault,
        sync,
    });
    let s2 = state.clone();
    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .manage(state)
        .setup(move |app| {
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
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            commands::get_app_info,
            commands::start_mock,
            commands::stop_mock,
            commands::get_snapshot,
            commands::list_accounts,
            commands::create_account,
            commands::update_account,
            commands::delete_account,
            commands::get_vault_status,
            commands::connect_account,
            commands::disconnect_account,
            commands::list_account_live
        ])
        .run(tauri::generate_context!())
        .expect("error while running DeSSS");
}
