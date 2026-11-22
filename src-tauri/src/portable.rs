use std::path::PathBuf;

/// Portable root: directory of the executable. All persistence lives in ./data next to the exe.
/// In dev (cargo run) current_exe is target/debug/desss(.exe) — data still sits next to that exe, never in OS config dirs.
#[derive(Clone, Debug)]
pub struct PortablePaths {
    pub root: PathBuf,
    pub data_dir: PathBuf,
    pub db_path: PathBuf,
    pub log_dir: PathBuf,
}

impl PortablePaths {
    pub fn resolve() -> Self {
        let exe = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("."));
        let root = exe
            .parent()
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| PathBuf::from("."));
        let data_dir = root.join("data");
        let log_dir = data_dir.join("logs");
        let db_path = data_dir.join("desss.db");
        Self {
            root,
            data_dir,
            db_path,
            log_dir,
        }
    }

    pub fn ensure_dirs(&self) -> std::io::Result<()> {
        std::fs::create_dir_all(&self.data_dir)?;
        std::fs::create_dir_all(&self.log_dir)?;
        Ok(())
    }
}

/// Install a panic hook that appends every panic (message, location and
/// backtrace) to ./data/logs/crash.log next to the exe, so a release build
/// with the Windows GUI subsystem never dies silently. Call this as the
/// very first thing in main()/run(), before any code that can panic.
pub fn install_panic_hook() {
    let paths = PortablePaths::resolve();
    let _ = paths.ensure_dirs();
    let crash_log = paths.log_dir.join("crash.log");
    std::panic::set_hook(Box::new(move |info| {
        let ts = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ");
        let backtrace = std::backtrace::Backtrace::force_capture();
        let msg = format!("[{ts}] PANIC {info}\nbacktrace:\n{backtrace}\n---\n");
        eprint!("{msg}");
        if let Ok(mut f) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&crash_log)
        {
            use std::io::Write;
            let _ = f.write_all(msg.as_bytes());
        }
    }));
}
