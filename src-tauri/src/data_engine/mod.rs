use rusqlite::{params, Connection};
use std::sync::Mutex;

pub mod realtime;

use crate::portable::PortablePaths;

/// One row of the `accounts` table. Metadata only — credentials live in the
/// encrypted vault, keyed by `id`, never in SQLite.
#[derive(Clone, Debug)]
pub struct AccountRow {
    pub id: String,
    pub label: String,
    pub exchange_id: String,
    pub masked_identifier: String,
    pub created_at: i64,
    pub updated_at: i64,
    pub status: String,
    pub credential_configured: bool,
}

pub struct DataEngine {
    pub conn: Mutex<Connection>,
    pub paths: PortablePaths,
}

impl DataEngine {
    pub fn open(paths: PortablePaths) -> Result<Self, Box<dyn std::error::Error>> {
        paths.ensure_dirs()?;
        let conn = Connection::open(&paths.db_path)?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT);
             CREATE TABLE IF NOT EXISTS orders(id TEXT PRIMARY KEY, symbol TEXT, side TEXT, qty REAL, status TEXT, ts INTEGER);
             CREATE TABLE IF NOT EXISTS ticks(symbol TEXT, last REAL, ts INTEGER);
             CREATE TABLE IF NOT EXISTS instruments(
               exchange_id TEXT NOT NULL,
               symbol TEXT NOT NULL,
               tick_size REAL,
               step_size REAL,
               min_notional REAL,
               max_leverage REAL,
               funding_interval_h REAL,
               settlement_currency TEXT NOT NULL DEFAULT '',
               market_id INTEGER,
               status TEXT NOT NULL DEFAULT 'trading',
               updated_at INTEGER NOT NULL,
               PRIMARY KEY (exchange_id, symbol)
             );
             CREATE TABLE IF NOT EXISTS accounts(
               id TEXT PRIMARY KEY,
               label TEXT NOT NULL,
               exchange_id TEXT NOT NULL,
               masked_identifier TEXT NOT NULL DEFAULT '',
               created_at INTEGER NOT NULL,
               updated_at INTEGER NOT NULL,
               status TEXT NOT NULL DEFAULT 'configured',
               credential_configured INTEGER NOT NULL DEFAULT 0
             );",
        )?;
        Ok(Self {
            conn: Mutex::new(conn),
            paths,
        })
    }

    pub(crate) fn lock(&self) -> std::sync::MutexGuard<'_, Connection> {
        self.conn.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Run `f` inside one SQLite transaction on the engine's
    /// connection. Store modules use this instead of touching the
    /// connection directly, so the transaction boundary stays here.
    pub fn transaction<T>(
        &self,
        f: impl FnOnce(&rusqlite::Transaction<'_>) -> Result<T, String>,
    ) -> Result<T, String> {
        let mut conn = self.lock();
        let tx = conn.transaction().map_err(|e| format!("事务失败：{e}"))?;
        let out = f(&tx)?;
        tx.commit().map_err(|e| format!("提交事务失败：{e}"))?;
        Ok(out)
    }

    /// One key/value from the `settings` table (risk config, UI prefs…).
    /// Missing keys return None — callers apply their own default.
    pub fn get_setting(&self, key: &str) -> Result<Option<String>, String> {
        let conn = self.lock();
        let mut stmt = conn
            .prepare("SELECT value FROM settings WHERE key = ?1")
            .map_err(|e| format!("准备设置查询失败：{e}"))?;
        let mut rows = stmt
            .query(params![key])
            .map_err(|e| format!("查询设置失败：{e}"))?;
        match rows.next() {
            Ok(Some(row)) => Ok(Some(
                row.get::<_, String>(0)
                    .map_err(|e| format!("读取设置失败：{e}"))?,
            )),
            Ok(None) => Ok(None),
            Err(e) => Err(format!("查询设置失败：{e}")),
        }
    }

    /// Upsert one key/value into the `settings` table.
    pub fn set_setting(&self, key: &str, value: &str) -> Result<(), String> {
        let conn = self.lock();
        conn.execute(
            "INSERT INTO settings(key, value) VALUES (?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![key, value],
        )
        .map_err(|e| format!("保存设置失败：{e}"))?;
        Ok(())
    }
}
