#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

fn main() {
    // Install the crash logger before anything else can panic:
    // every panic is appended to ./data/logs/crash.log next to the exe.
    desss_lib::portable::install_panic_hook();
    desss_lib::run();
}
