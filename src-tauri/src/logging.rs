//! File logging: `./data/logs/app.log`, 5 MB x 2 rolling, via the
//! existing `tracing` + `tracing-subscriber` (no new dependency).
//!
//! Levels: warn/error are always written; info only for the lines the
//! codebase emits deliberately (network / order / risk / strategy —
//! search, catalog and refresh instrumentation). There is no stdout
//! sink: the Windows GUI build has no console, and the in-app console
//! drawer already exists via the bus.
//!
//! The writer is best-effort: if the log file cannot be opened, lines
//! are discarded (with one `eprintln!`) and the app keeps running.

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// One log file holds at most this many bytes before rotation.
const MAX_BYTES: u64 = 5 * 1024 * 1024;

/// Append-only writer that rotates `app.log` to `app.log.1` (keeping
/// only the previous file) once it passes [`MAX_BYTES`].
struct RollingWriter {
    dir: PathBuf,
    file: Option<File>,
    written: u64,
}

impl RollingWriter {
    fn new(dir: &Path) -> Self {
        let path = dir.join("app.log");
        let written = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        let file = OpenOptions::new().create(true).append(true).open(&path);
        let file = match file {
            Ok(f) => Some(f),
            Err(e) => {
                eprintln!("[logging] cannot open {path:?}: {e}");
                None
            }
        };
        Self {
            dir: dir.to_path_buf(),
            file,
            written,
        }
    }

    fn rotate(&mut self) {
        let cur = self.dir.join("app.log");
        let prev = self.dir.join("app.log.1");
        let _ = std::fs::remove_file(&prev);
        let _ = std::fs::rename(&cur, &prev);
        self.file = OpenOptions::new().create(true).append(true).open(&cur).ok();
        self.written = 0;
    }
}

impl Write for RollingWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let Some(f) = self.file.as_mut() else {
            return Ok(buf.len());
        };
        if self.written + buf.len() as u64 > MAX_BYTES {
            self.rotate();
            let Some(f) = self.file.as_mut() else {
                return Ok(buf.len());
            };
            let n = f.write(buf)?;
            self.written += n as u64;
            return Ok(n);
        }
        let n = f.write(buf)?;
        self.written += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        match self.file.as_mut() {
            Some(f) => f.flush(),
            None => Ok(()),
        }
    }
}

/// Install the global tracing subscriber writing to
/// `./data/logs/app.log`. Safe to call once at startup; a second call
/// is a no-op. Never panics.
pub fn init(log_dir: &Path) {
    let writer = Mutex::new(RollingWriter::new(log_dir));
    let _ = tracing_subscriber::fmt()
        .with_writer(writer)
        .with_ansi(false)
        .with_env_filter("warn,desss_lib=info")
        .try_init();
}
