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
use bus::EventBus;
use portable::PortablePaths;

pub struct AppState {
    pub paths: PortablePaths,
    pub bus: EventBus,
    pub mock: Arc<MockAdapter>,
    pub db: data_engine::DataEngine,
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    // First thing: route every panic to ./data/logs/crash.log (portable dir),
    // so failures during init or in setup() are never silent.
    portable::install_panic_hook();

    let paths = PortablePaths::resolve();
    paths.ensure_dirs().expect("create ./data");
    let db = data_engine::DataEngine::open(paths.clone()).expect("open desss.db");
    let bus = EventBus::new(1024);
    let mock = Arc::new(MockAdapter::new(bus.clone()));
    let state = Arc::new(AppState {
        paths,
        bus,
        mock,
        db,
    });
    let s2 = state.clone();
    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .manage(state)
        .setup(move |app| {
            let handle = app.handle().clone();
            commands::spawn_bus_forwarder(handle, s2.clone());
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            commands::get_app_info,
            commands::start_mock,
            commands::stop_mock,
            commands::get_snapshot
        ])
        .run(tauri::generate_context!())
        .expect("error while running DeSSS");
}
