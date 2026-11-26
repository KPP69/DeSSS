use rusqlite::{params, Connection};
use std::sync::Mutex;

pub mod realtime;

use crate::core::market::Instrument;
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

    fn lock(&self) -> std::sync::MutexGuard<'_, Connection> {
        self.conn.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn insert_account(&self, row: &AccountRow) -> Result<(), String> {
        let conn = self.lock();
        conn.execute(
            "INSERT INTO accounts(id, label, exchange_id, masked_identifier, created_at, updated_at, status, credential_configured)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                row.id,
                row.label,
                row.exchange_id,
                row.masked_identifier,
                row.created_at,
                row.updated_at,
                row.status,
                row.credential_configured as i64
            ],
        )
        .map_err(|e| format!("写入账户失败：{e}"))?;
        Ok(())
    }

    pub fn get_account(&self, id: &str) -> Result<Option<AccountRow>, String> {
        let conn = self.lock();
        let mut stmt = conn
            .prepare(
                "SELECT id, label, exchange_id, masked_identifier, created_at, updated_at, status, credential_configured
                 FROM accounts WHERE id = ?1",
            )
            .map_err(|e| format!("准备账户查询失败：{e}"))?;
        let rows = stmt
            .query_map(params![id], map_account_row)
            .map_err(|e| format!("查询账户失败：{e}"))?;
        for row in rows {
            return Ok(Some(row.map_err(|e| format!("读取账户失败：{e}"))?));
        }
        Ok(None)
    }

    pub fn list_accounts(&self) -> Result<Vec<AccountRow>, String> {
        let conn = self.lock();
        let mut stmt = conn
            .prepare(
                "SELECT id, label, exchange_id, masked_identifier, created_at, updated_at, status, credential_configured
                 FROM accounts ORDER BY created_at ASC, label ASC",
            )
            .map_err(|e| format!("准备账户列表查询失败：{e}"))?;
        let rows = stmt
            .query_map([], map_account_row)
            .map_err(|e| format!("列出账户失败：{e}"))?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row.map_err(|e| format!("读取账户失败：{e}"))?);
        }
        Ok(out)
    }

    /// Replace one exchange's instruments wholesale (bulk refresh):
    /// delete its old rows, insert the new set, in one transaction.
    /// Quotes are never stored — only static instrument fields.
    pub fn replace_instruments(
        &self,
        exchange_id: &str,
        instruments: &[Instrument],
    ) -> Result<(), String> {
        let mut conn = self.lock();
        let tx = conn
            .transaction()
            .map_err(|e| format!("写入目录失败：{e}"))?;
        tx.execute(
            "DELETE FROM instruments WHERE exchange_id = ?1",
            params![exchange_id],
        )
        .map_err(|e| format!("写入目录失败：{e}"))?;
        {
            let mut stmt = tx
                .prepare(
                    "INSERT INTO instruments(exchange_id, symbol, tick_size, step_size, min_notional, max_leverage, funding_interval_h, settlement_currency, market_id, status, updated_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                )
                .map_err(|e| format!("写入目录失败：{e}"))?;
            for ins in instruments {
                stmt.execute(params![
                    ins.exchange_id,
                    ins.symbol,
                    ins.tick_size,
                    ins.step_size,
                    ins.min_notional,
                    ins.max_leverage,
                    ins.funding_interval_h,
                    ins.settlement_currency,
                    ins.market_id.map(|m| m as i64),
                    ins.status,
                    ins.updated_at,
                ])
                .map_err(|e| format!("写入目录失败：{e}"))?;
            }
        }
        tx.commit().map_err(|e| format!("写入目录失败：{e}"))?;
        Ok(())
    }

    /// All stored instruments, for the cold-start catalog load.
    pub fn load_instruments(&self) -> Result<Vec<Instrument>, String> {
        let conn = self.lock();
        let mut stmt = conn
            .prepare(
                "SELECT exchange_id, symbol, tick_size, step_size, min_notional, max_leverage, funding_interval_h, settlement_currency, market_id, status, updated_at
                 FROM instruments ORDER BY exchange_id, symbol",
            )
            .map_err(|e| format!("读取目录失败：{e}"))?;
        let rows = stmt
            .query_map([], |row| {
                Ok(Instrument {
                    exchange_id: row.get(0)?,
                    symbol: row.get(1)?,
                    tick_size: row.get(2)?,
                    step_size: row.get(3)?,
                    min_notional: row.get(4)?,
                    max_leverage: row.get(5)?,
                    funding_interval_h: row.get(6)?,
                    settlement_currency: row.get(7)?,
                    market_id: row.get::<_, Option<i64>>(8)?.map(|m| m as u64),
                    status: row.get(9)?,
                    updated_at: row.get(10)?,
                })
            })
            .map_err(|e| format!("读取目录失败：{e}"))?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row.map_err(|e| format!("读取目录失败：{e}"))?);
        }
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

    /// Update mutable metadata. `exchange_id` is deliberately NOT updatable:
    /// an account's exchange is locked at creation time.
    pub fn update_account_meta(
        &self,
        id: &str,
        label: &str,
        masked_identifier: &str,
        credential_configured: bool,
        updated_at: i64,
    ) -> Result<(), String> {
        let conn = self.lock();
        let changed = conn
            .execute(
                "UPDATE accounts SET label = ?2, masked_identifier = ?3, credential_configured = ?4, updated_at = ?5 WHERE id = ?1",
                params![id, label, masked_identifier, credential_configured as i64, updated_at],
            )
            .map_err(|e| format!("更新账户失败：{e}"))?;
        if changed == 0 {
            return Err("找不到该账户".to_string());
        }
        Ok(())
    }

    pub fn delete_account(&self, id: &str) -> Result<(), String> {
        let conn = self.lock();
        let changed = conn
            .execute("DELETE FROM accounts WHERE id = ?1", params![id])
            .map_err(|e| format!("删除账户失败：{e}"))?;
        if changed == 0 {
            return Err("找不到该账户".to_string());
        }
        Ok(())
    }
}

fn map_account_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<AccountRow> {
    Ok(AccountRow {
        id: row.get(0)?,
        label: row.get(1)?,
        exchange_id: row.get(2)?,
        masked_identifier: row.get(3)?,
        created_at: row.get(4)?,
        updated_at: row.get(5)?,
        status: row.get(6)?,
        credential_configured: row.get::<_, i64>(7)? != 0,
    })
}
