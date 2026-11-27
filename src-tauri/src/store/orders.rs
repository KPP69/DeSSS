//! In-memory order records + fills (the execution engine owns
//! them; the exchange is the source of truth, and the store is
//! rebuilt from it on demand). Nothing here is persisted.

use std::collections::{HashMap, HashSet};
use std::sync::RwLock;
use std::time::Instant;

use crate::core::types::{Order, OrderInfo, OrderStatus, TradeInfo};

/// D10: one applied fill — (exchange order id, filled-size bits,
/// exchange time). Size is stored as bits because f64 has no Eq.
type AppliedFillKey = (u64, u64, i64);

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
    /// D10: fills already applied to the local position, per account.
    /// Keyed (oid, size bits, exchange time); a place-order fill is
    /// recorded with time 0 (the response carries none), so the
    /// userFills copy of the same execution is recognised by
    /// (oid, size, 0). Cleared by the next snapshot (§5).
    applied_fills: std::sync::Mutex<HashMap<String, HashSet<AppliedFillKey>>>,
    /// D10 §4/§5: local receipt time of the latest snapshot, per
    /// account (monotonic clock — exchange timestamps drift).
    last_snapshot_at: std::sync::Mutex<HashMap<String, Instant>>,
}

impl OrderStore {
    pub fn new() -> Self {
        Self {
            orders: RwLock::new(HashMap::new()),
            fills: RwLock::new(HashMap::new()),
            unresolved_attempts: std::sync::Mutex::new(HashMap::new()),
            applied_fills: std::sync::Mutex::new(HashMap::new()),
            last_snapshot_at: std::sync::Mutex::new(HashMap::new()),
        }
    }

    /// D10: record a fill as applied; true when it is new (the caller
    /// applies it), false on a replay or on the place-order copy of a
    /// fill the WS already delivered (the frame can beat the HTTP
    /// response, so a time-0 record also matches any earlier copy).
    pub fn note_applied_fill(&self, account_id: &str, oid: u64, sz: f64, time: i64) -> bool {
        let key = (oid, sz.to_bits(), time);
        let Ok(mut m) = self.applied_fills.lock() else {
            return false;
        };
        let set = m.entry(account_id.to_string()).or_default();
        let seen = set.contains(&key)
            || set.contains(&(oid, sz.to_bits(), 0))
            || (time == 0 && set.iter().any(|(o, s, _)| *o == oid && *s == sz.to_bits()));
        if seen {
            return false;
        }
        set.insert(key);
        true
    }

    /// D10 §4: only apply fills received after the latest snapshot.
    /// A fill received before it is already inside it.
    pub fn fill_fresh(&self, account_id: &str, now: Instant) -> bool {
        let Ok(m) = self.last_snapshot_at.lock() else {
            return true;
        };
        m.get(account_id).is_none_or(|&t| now > t)
    }

    /// D10 §5: a snapshot replaces the base — forget applied fills
    /// and stamp its local receipt time.
    pub fn note_snapshot(&self, account_id: &str, now: Instant) {
        if let Ok(mut m) = self.applied_fills.lock() {
            m.remove(account_id);
        }
        if let Ok(mut m) = self.last_snapshot_at.lock() {
            m.insert(account_id.to_string(), now);
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

    /// Append fills for one order, keyed by trade id: the WS
    /// userFills channel sends a snapshot on subscribe and then
    /// live fills, so the same fill can arrive twice — a second
    /// copy must never duplicate the record (Phase 3.3).
    pub fn append_fills(&self, cloid: &str, fills: Vec<TradeInfo>) {
        if fills.is_empty() {
            return;
        }
        if let Ok(mut m) = self.fills.write() {
            let entry = m.entry(cloid.to_string()).or_default();
            for f in fills {
                if !entry.iter().any(|e| e.trade_id == f.trade_id) {
                    entry.push(f);
                }
            }
        }
    }

    /// Merge the exchange's live open orders into the store (pure step,
    /// no network) and return the live list plus the local non-terminal
    /// records that are no longer on the exchange. Moved here from
    /// `execution` (Phase 3.3): the account WS applies its openOrders
    /// frames through the same merge, and the store owns the merge.
    ///
    /// - Each exchange row is authoritative for open state: a stored
    ///   record matching by oid is refreshed to Open; a row with no
    ///   stored record (an order placed on the website, or before a
    ///   restart) is inserted as an Open record, so it shows up and can
    ///   be cancelled. A website order without a cloid is keyed by its
    ///   oid string, exactly like the cancel flow's synthetic record.
    /// - Local records in Open / Submitted / PartiallyFilled with a real
    ///   oid that are missing from the live list are returned for the
    ///   caller to query — they may have been filled or canceled
    ///   elsewhere. Terminal records and records without an oid (never
    ///   sent) are never reported missing. Unresolved is deliberately
    ///   excluded: it already has its own reconcile flow and must never
    ///   be cleaned up by a list diff.
    pub fn merge_open_orders(
        &self,
        account_id: &str,
        exchange: &[Order],
    ) -> (Vec<OrderInfo>, Vec<OrderInfo>) {
        let mut live = Vec::new();
        let mut live_oids: HashSet<&str> = HashSet::new();
        for o in exchange {
            live_oids.insert(o.order_id.as_str());
            let row = if let Some(mut stored) = self.get_by_oid(&o.order_id) {
                // The exchange says it is open; our record may be stale
                // (e.g. a cancel that raced). Trust the exchange.
                stored
                    .transition(OrderStatus::Open, crate::core::util::now_ms())
                    .ok();
                stored.updated_at = crate::core::util::now_ms();
                stored
            } else {
                OrderInfo {
                    order_id: o.order_id.clone(),
                    client_order_id: if o.client_order_id.is_empty() {
                        o.order_id.clone()
                    } else {
                        o.client_order_id.clone()
                    },
                    exchange_id: o.exchange_id.clone(),
                    account_id: o.account_id.clone(),
                    symbol: o.symbol.clone(),
                    side: o.side.clone(),
                    order_type: o.order_type.clone(),
                    status: OrderStatus::Open,
                    price: o.price,
                    quantity: o.quantity,
                    filled_quantity: 0.0,
                    remaining_quantity: o.quantity,
                    average_fill_price: None,
                    error: None,
                    trigger_px: o.trigger_px,
                    tpsl: o.tpsl.clone(),
                    // Imported from the exchange (e.g. placed on the
                    // website): a human order, never a strategy order.
                    origin: "manual".to_string(),
                    created_at: o.ts,
                    updated_at: o.ts,
                }
            };
            self.insert(row.clone());
            live.push(row);
        }
        let missing: Vec<OrderInfo> = self
            .list()
            .into_iter()
            .filter(|r| {
                r.account_id == account_id
                    && matches!(
                        r.status,
                        OrderStatus::Open | OrderStatus::Submitted | OrderStatus::PartiallyFilled
                    )
                    && !r.order_id.is_empty()
                    && !live_oids.contains(r.order_id.as_str())
            })
            .collect();
        (live, missing)
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
                // Origin is local bookkeeping, never exchange state:
                // a query result must not overwrite it (a strategy
                // order stays "strategy:<id>" after reconciliation).
                merged.origin = stored.origin.clone();
                // Same for the trigger fields: the exchange may not
                // report tp/sl (plain openOrders gives "Trigger
                // Market"), but the placement intent knew them.
                if merged.trigger_px.is_none() {
                    merged.trigger_px = stored.trigger_px;
                }
                if merged.tpsl.is_none() {
                    merged.tpsl = stored.tpsl.clone();
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
