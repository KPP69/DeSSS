//! The `instruments` table: static instrument fields only.
//! Quotes are never stored — only the catalog rows.

use rusqlite::params;

use crate::core::market::Instrument;
use crate::data_engine::DataEngine;

impl DataEngine {
    /// Replace one exchange's instruments wholesale (bulk refresh):
    /// delete its old rows, insert the new set, in one transaction.
    /// Quotes are never stored — only static instrument fields.
    pub fn replace_instruments(
        &self,
        exchange_id: &str,
        instruments: &[Instrument],
    ) -> Result<(), String> {
        self.transaction(|tx| {
            tx.execute(
                "DELETE FROM instruments WHERE exchange_id = ?1",
                params![exchange_id],
            )
            .map_err(|e| format!("写入目录失败：{e}"))?;
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
            Ok(())
        })
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
}
