//! In-memory order records + fills (the execution engine owns
//! them; the exchange is the source of truth, and the store is
//! rebuilt from it on demand). Nothing here is persisted.

use std::collections::HashMap;
use std::sync::RwLock;

use crate::core::types::{OrderInfo, OrderStatus, TradeInfo};

/// In-memory order store: the execution engine owns it. Orders are
/// keyed by client order id (cloid); fills are kept per cloid.
/// Nothing here is persisted — the exchange is the source of truth,
/// and the store is rebuilt from it on demand.
pub struct OrderStore {
    orders: RwLock<HashMap<String, OrderInfo>>,
    fills: RwLock<HashMap<String, Vec<TradeInfo>>>,
    /// `unknownOid` strikes per cloid (F4): (count, last strike ms).
    /// In-memory, cumulative, reset on restart.
    unresolved_attempts: std::sync::Mutex<HashMap<String, (u32, i64)>>,
}

impl OrderStore {
    pub fn new() -> Self {
        Self {
            orders: RwLock::new(HashMap::new()),
            fills: RwLock::new(HashMap::new()),
            unresolved_attempts: std::sync::Mutex::new(HashMap::new()),
        }
    }

    pub fn insert(&self, info: OrderInfo) {
        if let Ok(mut m) = self.orders.write() {
            m.insert(info.client_order_id.clone(), info);
        }
    }

    pub fn get(&self, cloid: &str) -> Option<OrderInfo> {
        self.orders.read().ok().and_then(|m| m.get(cloid).cloned())
    }

    pub fn get_by_oid(&self, oid: &str) -> Option<OrderInfo> {
        self.orders
            .read()
            .ok()?
            .values()
            .find(|o| !o.order_id.is_empty() && o.order_id == oid)
            .cloned()
    }

    /// All records, newest first.
    pub fn list(&self) -> Vec<OrderInfo> {
        let mut out: Vec<OrderInfo> = self
            .orders
            .read()
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default();
        out.sort_by_key(|o| -o.created_at);
        out
    }

    /// Non-terminal records (Open / PartiallyFilled / Submitted /
    /// Unresolved), newest first.
    pub fn list_open(&self) -> Vec<OrderInfo> {
        self.list()
            .into_iter()
            .filter(|o| !o.status.is_terminal())
            .collect()
    }

    /// All recorded fills, newest first.
    pub fn list_fills(&self) -> Vec<TradeInfo> {
        let mut out: Vec<TradeInfo> = self
            .fills
            .read()
            .map(|m| m.values().flatten().cloned().collect())
            .unwrap_or_default();
        out.sort_by_key(|f| -f.timestamp);
        out
    }

    pub fn record_fills(&self, cloid: &str, fills: Vec<TradeInfo>) {
        if fills.is_empty() {
            return;
        }
        if let Ok(mut m) = self.fills.write() {
            m.insert(cloid.to_string(), fills);
        }
    }

    /// True when an order with the same account + symbol + side +
    /// quantity + price is Unresolved — resubmitting it would be a
    /// duplicate while its outcome is unknown.
    pub fn has_unresolved_duplicate(
        &self,
        account_id: &str,
        symbol: &str,
        side: &str,
        qty: f64,
        price: f64,
    ) -> Option<String> {
        self.orders
            .read()
            .ok()?
            .values()
            .find(|o| {
                o.status == OrderStatus::Unresolved
                    && o.account_id == account_id
                    && o.symbol == symbol
                    && o.side == side
                    && (o.quantity - qty).abs() < 1e-12
                    && o.price.map(|p| (p - price).abs() < 1e-12).unwrap_or(false)
            })
            .map(|o| o.client_order_id.clone())
    }

    /// Count one `unknownOid` for an Unresolved order (F4); returns
    /// the running total. Only an explicit `unknownOid` reaches
    /// here — transport / HTTP / parse failures never call it.
    pub fn unknown_oid_strike(&self, cloid: &str, now_ms: i64) -> u32 {
        if let Ok(mut m) = self.unresolved_attempts.lock() {
            let e = m.entry(cloid.to_string()).or_insert((0, 0));
            e.0 += 1;
            e.1 = now_ms;
            e.0
        } else {
            0
        }
    }

    /// Forget the strike entry when an order leaves Unresolved (F4).
    pub fn clear_unknown_oid(&self, cloid: &str) {
        if let Ok(mut m) = self.unresolved_attempts.lock() {
            m.remove(cloid);
        }
    }

    /// Drop strike entries not updated for over 10 minutes (F4):
    /// the table must not grow with runtime.
    pub fn prune_unknown_oid_strikes(&self, now_ms: i64) {
        if let Ok(mut m) = self.unresolved_attempts.lock() {
            m.retain(|_, (_, ts)| now_ms - *ts <= 600_000);
        }
    }

    /// Apply an authoritative query result to the stored record
    /// (status + fills + quantities). The queried record wins; when
    /// the store has no such order (placed outside this app) it is
    /// inserted as-is. Returns the merged record.
    pub fn apply_query(&self, queried: &OrderInfo) -> OrderInfo {
        if let Ok(mut m) = self.orders.write() {
            if let Some(stored) = m.get_mut(&queried.client_order_id) {
                // Keep the original intent's price/qty when the query
                // cannot see them (unknownOid never reaches here).
                let mut merged = queried.clone();
                if merged.quantity == 0.0 {
                    merged.quantity = stored.quantity;
                }
                // F4: leaving Unresolved for a terminal state clears
                // the strike entry, whatever path applied the update.
                if stored.status == OrderStatus::Unresolved && merged.status.is_terminal() {
                    self.clear_unknown_oid(&queried.client_order_id);
                }
                *stored = merged.clone();
                return merged;
            }
            m.insert(queried.client_order_id.clone(), queried.clone());
        }
        queried.clone()
    }
}
