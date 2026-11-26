//! Per-account real-exchange sync manager (phase 3).
//!
//! Owns the running adapters (one per connected account) and the latest
//! live snapshot map the frontend reads through `list_account_live`.
//! Adapters are built from the account row + vault credentials — only the
//! public identifiers are used (wallet address for Hyperliquid/Entropy,
//! account index for Lighter, wallet address for Arcus). Private keys are
//! never read here and never leave the vault. AsterDEX is the exception
//! that needs a secret: an Agent wallet's private key is passed to the
//! adapter for V3 EIP-712 signing only; it is never logged.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, RwLock};

use serde_json::Value;

use super::reconcile::ReconcileState;
use super::ExchangeAdapter;
use crate::bus::EventBus;
use crate::core::types::{AccountLive, LivePosition};
use crate::data_engine::AccountRow;

pub struct SyncManager {
    live: Arc<RwLock<HashMap<String, AccountLive>>>,
    running: Mutex<HashMap<String, Arc<dyn ExchangeAdapter>>>,
    /// Join handles of the spawned run loops, one per connected account.
    /// Aborting the handle is what makes delete/disconnect cleanup
    /// synchronous: without it the run task would keep its adapter clone
    /// (and the credentials inside it) alive until the next poll tick.
    tasks: Mutex<HashMap<String, tauri::async_runtime::JoinHandle<()>>>,
    /// Post-fill reconciliation grace (spec §3): shared with every
    /// adapter so their poll loops skip writes inside the window.
    reconcile: Arc<ReconcileState>,
}

impl SyncManager {
    pub fn new() -> Self {
        Self {
            live: Arc::new(RwLock::new(HashMap::new())),
            running: Mutex::new(HashMap::new()),
            tasks: Mutex::new(HashMap::new()),
            reconcile: Arc::new(ReconcileState::new()),
        }
    }

    pub fn reconcile_state(&self) -> Arc<ReconcileState> {
        self.reconcile.clone()
    }

    pub fn adapter(&self, account_id: &str) -> Option<Arc<dyn ExchangeAdapter>> {
        self.running
            .lock()
            .ok()
            .and_then(|m| m.get(account_id).cloned())
    }

    /// Record a fill for an account: starts its 5s reconciliation
    /// grace window (spec §3).
    pub fn note_fill(&self, account_id: &str) {
        self.reconcile.note_fill(account_id);
    }

    /// Apply a fill to the live positions immediately (local estimate)
    /// and start the grace window. The exchange-confirmed state
    /// remains the source of truth: the next post-grace poll overwrites
    /// this estimate with confirmed positions. Equity / available are
    /// left to that poll — they need the exchange's margin math.
    pub fn apply_fill(
        &self,
        account_id: &str,
        symbol: &str,
        side: &str,
        qty: f64,
        fill_price: f64,
    ) {
        self.reconcile.note_fill(account_id);
        let Ok(mut live) = self.live.write() else {
            return;
        };
        let Some(entry) = live.get_mut(account_id) else {
            return;
        };
        // Signed net quantity: buys add, sells subtract.
        let signed = if side == "buy" { qty } else { -qty };
        if signed.abs() < 1e-12 {
            return;
        }
        let idx = entry.positions.iter().position(|p| p.symbol == symbol);
        match idx {
            Some(i) => {
                let p = entry.positions[i].clone();
                let cur_signed = if p.side == "long" { p.qty } else { -p.qty };
                let net = cur_signed + signed;
                if net.abs() < 1e-12 {
                    entry.positions.remove(i);
                    return;
                }
                let new_side = if net > 0.0 { "long" } else { "short" };
                let (new_entry, new_mark, new_pnl) =
                    if new_side == p.side && net.abs() > cur_signed.abs() {
                        // Increased: volume-weighted average entry price.
                        let added = signed.abs();
                        (
                            (p.entry_price * cur_signed.abs() + fill_price * added) / net.abs(),
                            p.mark_price,
                            p.unrealized_pnl,
                        )
                    } else if new_side != p.side {
                        // Flipped: the new side starts at the fill price.
                        (fill_price, fill_price, 0.0)
                    } else {
                        // Reduced: entry price unchanged.
                        (p.entry_price, p.mark_price, p.unrealized_pnl)
                    };
                let pos = &mut entry.positions[i];
                pos.side = new_side.to_string();
                pos.qty = net.abs();
                pos.entry_price = new_entry;
                pos.mark_price = new_mark;
                pos.unrealized_pnl = new_pnl;
            }
            None => {
                entry.positions.push(LivePosition {
                    symbol: symbol.to_string(),
                    side: if signed > 0.0 {
                        "long".to_string()
                    } else {
                        "short".to_string()
                    },
                    qty: signed.abs(),
                    entry_price: fill_price,
                    mark_price: fill_price,
                    unrealized_pnl: 0.0,
                });
            }
        }
    }

    /// Force a position reconciliation for one account, after the
    /// post-fill grace expires. The poll itself runs in the adapter's
    /// own loop (woken via `wake_sync`), so the fetch path and the
    /// failure semantics are exactly the loop's: a failed query keeps
    /// the last known positions, never zeroes them.
    pub fn reconcile_account(self: &Arc<Self>, account_id: &str) {
        let account_id = account_id.to_string();
        let this = Arc::clone(self);
        // `reconcile_account` is a sync fn callable from any thread, so the
        // task must go through `tauri::async_runtime::spawn` — `tokio::spawn`
        // panics outside a Tokio context (real-machine crash 2026-11-26).
        tauri::async_runtime::spawn(async move {
            let wait = this.reconcile.grace_remaining_ms(&account_id);
            if wait > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(wait as u64)).await;
            }
            if let Some(adapter) = this.adapter(&account_id) {
                adapter.wake_sync();
            }
        });
    }

    /// Reconcile every connected account of one exchange (used when a
    /// WS stream for that exchange reconnects after a drop).
    pub fn reconcile_exchange(self: &Arc<Self>, exchange_id: &str) {
        for live in self.live_states() {
            if live.adapter_id == exchange_id {
                self.reconcile_account(&live.account_id);
            }
        }
    }

    pub fn live_map(&self) -> Arc<RwLock<HashMap<String, AccountLive>>> {
        self.live.clone()
    }

    pub fn live_states(&self) -> Vec<AccountLive> {
        self.live
            .read()
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default()
    }

    /// Total equity across live accounts, counting each underlying
    /// wallet once (see `AccountLive::wallet_key`): Hyperliquid main
    /// and Entropy report the same aggregated wallet, so a plain sum
    /// would double it. Used by the 10s drawdown sampler.
    pub fn total_equity(&self) -> f64 {
        let mut seen = HashSet::new();
        self.live_states()
            .iter()
            .filter(|s| seen.insert(s.wallet_key.clone().unwrap_or_else(|| s.account_id.clone())))
            .map(|s| s.equity_usd)
            .sum()
    }

    pub fn is_running(&self, account_id: &str) -> bool {
        self.running
            .lock()
            .map(|m| m.contains_key(account_id))
            .unwrap_or(false)
    }

    /// (account_id, exchange_id) of every connected account, for the
    /// periodic open-orders sync (the background task in `lib.rs`
    /// filters it to the order-capable exchanges).
    pub fn connected_accounts(&self) -> Vec<(String, String)> {
        self.running
            .lock()
            .map(|m| {
                m.iter()
                    .map(|(id, a)| (id.clone(), a.id().to_string()))
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn insert(&self, account_id: String, adapter: Arc<dyn ExchangeAdapter>) {
        if let Ok(mut m) = self.running.lock() {
            m.insert(account_id, adapter);
        }
    }

    pub fn remove(&self, account_id: &str) -> Option<Arc<dyn ExchangeAdapter>> {
        self.running
            .lock()
            .ok()
            .and_then(|mut m| m.remove(account_id))
    }

    /// Register the spawned run loop for an account (called right after
    /// the spawn, so a delete landing at any later point can abort it).
    pub fn register_task(&self, account_id: String, handle: tauri::async_runtime::JoinHandle<()>) {
        if let Ok(mut m) = self.tasks.lock() {
            m.insert(account_id, handle);
        }
    }

    /// Abort the run loop for an account, if one is registered, and drop
    /// its handle: after this returns no task holds the adapter anymore.
    pub fn abort_task(&self, account_id: &str) {
        let handle = self
            .tasks
            .lock()
            .ok()
            .and_then(|mut m| m.remove(account_id));
        if let Some(h) = handle {
            h.abort();
        }
    }

    pub fn remove_live(&self, account_id: &str) {
        if let Ok(mut m) = self.live.write() {
            m.remove(account_id);
        }
    }
}

/// Read a 0x-prefixed 42-char wallet address from stored credentials.
pub(crate) fn valid_address(credentials: &Value, key: &str, label: &str) -> Result<String, String> {
    let address = credentials
        .get(key)
        .and_then(|v| v.as_str())
        .ok_or_else(|| format!("保存的凭证缺少{label}"))?
        .to_string();
    if !address.starts_with("0x") || address.len() != 42 {
        return Err(format!("{label}格式不正确（应为 0x 开头的 42 位地址）"));
    }
    Ok(address)
}

/// Build a read-only adapter for one stored account. The venue
/// registry owns every exchange now (PR-1.3 commit 3): an id the
/// registry does not know is "未知交易所", never a silent fallback.
/// AsterDEX still fails fast on stored API Key-mode credentials
/// (retired with V1: migration error) — that check lives in the
/// venue's own build.
pub fn build_adapter(
    row: &AccountRow,
    credentials: &Value,
    bus: EventBus,
    live: Arc<RwLock<HashMap<String, AccountLive>>>,
    reconcile: Arc<ReconcileState>,
) -> Result<Arc<dyn ExchangeAdapter>, String> {
    let account_id = row.id.clone();
    let Some(v) = crate::venues::all()
        .iter()
        .find(|v| v.id() == row.exchange_id.as_str())
    else {
        return Err(format!("未知交易所：{}", row.exchange_id));
    };
    v.build(
        credentials,
        &crate::venues::VenueCtx {
            account_id: account_id.clone(),
            bus: bus.clone(),
            live: Arc::clone(&live),
            reconcile: Arc::clone(&reconcile),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::LivePosition;

    fn live_with_positions() -> AccountLive {
        AccountLive {
            account_id: "acc-1".to_string(),
            adapter_id: "hyperliquid".to_string(),
            wallet_key: None,
            status: "connected".to_string(),
            latency_ms: Some(120),
            equity_usd: 10000.0,
            available_usd: 8000.0,
            pnl_usd: Some(0.0),
            currency: "USDC".to_string(),
            positions: vec![LivePosition {
                symbol: "BTC".to_string(),
                side: "long".to_string(),
                qty: 0.1,
                entry_price: 90000.0,
                mark_price: 91000.0,
                unrealized_pnl: 100.0,
            }],
            error: None,
            ts: 0,
        }
    }

    fn manager_with_live() -> SyncManager {
        let m = SyncManager::new();
        if let Ok(mut map) = m.live.write() {
            map.insert("acc-1".to_string(), live_with_positions());
        }
        m
    }

    fn asterdex_row() -> AccountRow {
        AccountRow {
            id: "acc-1".to_string(),
            label: "AsterDEX".to_string(),
            exchange_id: "asterdex".to_string(),
            masked_identifier: "0x…".to_string(),
            created_at: 0,
            updated_at: 0,
            status: "connected".to_string(),
            credential_configured: true,
        }
    }

    /// D3: a stored API Key-mode AsterDEX account has no
    /// wallet_address, so build fails on the missing field — the
    /// mode check and its migration error are deleted. An agent-mode
    /// account with missing fields fails with the field error
    /// instead, proving the dispatch reaches the agent path.
    #[test]
    fn asterdex_old_api_blob_build_fails_with_missing_field_error() {
        let row = asterdex_row();
        let api_creds = serde_json::json!({
            "mode": "api", "api_key": "key", "api_secret": "secret"
        });
        let err = match build_adapter(
            &row,
            &api_creds,
            EventBus::new(16),
            Arc::new(RwLock::new(HashMap::new())),
            Arc::new(ReconcileState::new()),
        ) {
            Ok(_) => panic!("api-mode build must fail"),
            Err(e) => e,
        };
        assert!(
            err.contains("缺少主钱包地址"),
            "field error expected, not migration: {err}"
        );
        assert!(!err.contains("已停用"), "{err}");
        let agent_creds = serde_json::json!({"mode": "agent"});
        let err = match build_adapter(
            &row,
            &agent_creds,
            EventBus::new(16),
            Arc::new(RwLock::new(HashMap::new())),
            Arc::new(ReconcileState::new()),
        ) {
            Ok(_) => panic!("agent-mode build without fields must fail"),
            Err(e) => e,
        };
        assert!(
            err.contains("缺少主钱包地址"),
            "field error expected, not migration: {err}"
        );
    }

    #[test]
    fn fill_estimate_opens_increases_reduces_and_closes() {
        let m = manager_with_live();
        // Increase the long: 0.1 @ 90000 + 0.1 @ 92000 → avg entry 91000.
        m.apply_fill("acc-1", "BTC", "buy", 0.1, 92000.0);
        let live = m.live_states().into_iter().next().unwrap();
        assert_eq!(live.positions.len(), 1);
        assert_eq!(live.positions[0].side, "long");
        assert!((live.positions[0].qty - 0.2).abs() < 1e-12);
        assert!((live.positions[0].entry_price - 91000.0).abs() < 1e-9);
        // Reduce: sell 0.05 → 0.15 left, entry unchanged.
        m.apply_fill("acc-1", "BTC", "sell", 0.05, 93000.0);
        let live = m.live_states().into_iter().next().unwrap();
        assert!((live.positions[0].qty - 0.15).abs() < 1e-12);
        assert!((live.positions[0].entry_price - 91000.0).abs() < 1e-9);
        // Close the rest: the position disappears.
        m.apply_fill("acc-1", "BTC", "sell", 0.15, 93000.0);
        let live = m.live_states().into_iter().next().unwrap();
        assert!(live.positions.is_empty());
        // A fill on an unknown account is a no-op, never a panic.
        m.apply_fill("acc-9", "BTC", "buy", 0.1, 90000.0);
    }

    #[test]
    fn fill_estimate_flips_side_and_opens_new_symbol() {
        let m = manager_with_live();
        // Sell 0.3 against a 0.1 long → 0.2 short at the fill price.
        m.apply_fill("acc-1", "BTC", "sell", 0.3, 88000.0);
        let live = m.live_states().into_iter().next().unwrap();
        assert_eq!(live.positions[0].side, "short");
        assert!((live.positions[0].qty - 0.2).abs() < 1e-12);
        assert_eq!(live.positions[0].entry_price, 88000.0);
        // A new symbol opens a fresh position.
        m.apply_fill("acc-1", "ETH", "buy", 1.0, 3000.0);
        let live = m.live_states().into_iter().next().unwrap();
        assert_eq!(live.positions.len(), 2);
    }

    #[test]
    fn fill_starts_the_reconciliation_grace() {
        let m = SyncManager::new();
        assert!(!m.reconcile.in_grace("acc-1"));
        m.note_fill("acc-1");
        assert!(m.reconcile.in_grace("acc-1"));
    }

    #[test]
    fn total_equity_counts_each_wallet_once() {
        let m = SyncManager::new();
        // Two accounts on the same wallet (HL main + Entropy): both
        // report the aggregated 10000, counted once.
        let mut main = live_with_positions();
        main.account_id = "acc-hl".to_string();
        main.wallet_key = Some("0xwallet".to_string());
        let mut entropy = live_with_positions();
        entropy.account_id = "acc-io".to_string();
        entropy.wallet_key = Some("0xwallet".to_string());
        // A third account with its own funds counts separately.
        let mut other = live_with_positions();
        other.account_id = "acc-arcus".to_string();
        other.equity_usd = 5000.0;
        if let Ok(mut map) = m.live.write() {
            map.insert(main.account_id.clone(), main);
            map.insert(entropy.account_id.clone(), entropy);
            map.insert(other.account_id.clone(), other);
        }
        assert_eq!(m.total_equity(), 15000.0);
    }
}
