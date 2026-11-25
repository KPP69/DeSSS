use rusqlite::{params, Connection};
use std::sync::Mutex;

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
