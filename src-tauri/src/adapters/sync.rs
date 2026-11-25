//! Per-account real-exchange sync manager (phase 3).
//!
//! Owns the running adapters (one per connected account) and the latest
//! live snapshot map the frontend reads through `list_account_live`.
//! Adapters are built from the account row + vault credentials — only the
//! public identifiers are used (wallet address for Hyperliquid/Entropy,
//! account index for Lighter, wallet address for Arcus). Private keys are
//! never read here and never leave the vault. AsterDEX is the exception
//! that needs a secret: its API Key/Secret pair is passed to the adapter
//! for HMAC signing only, and an Agent wallet's private key for V3 EIP-712
//! signing only; neither is ever logged.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use serde_json::Value;

use super::arcus::ArcusAdapter;
use super::asterdex::AsterdexAdapter;
use super::hyperliquid::HyperliquidAdapter;
use super::lighter::{LighterAdapter, LighterNetwork};
use super::reconcile::ReconcileState;
use super::types::{AccountLive, LivePosition};
use super::ExchangeAdapter;
use crate::bus::EventBus;
use crate::data_engine::AccountRow;

pub struct SyncManager {
    live: Arc<RwLock<HashMap<String, AccountLive>>>,
    running: Mutex<HashMap<String, Arc<dyn ExchangeAdapter>>>,
    /// Join handles of the spawned run loops, one per connected account.
    /// Aborting the handle is what makes delete/disconnect cleanup
    /// synchronous: without it the run task would keep its adapter clone
    /// (and the credentials inside it) alive until the next poll tick.
    tasks: Mutex<HashMap<String, tokio::task::JoinHandle<()>>>,
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
        tokio::spawn(async move {
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

    pub fn is_running(&self, account_id: &str) -> bool {
        self.running
            .lock()
            .map(|m| m.contains_key(account_id))
            .unwrap_or(false)
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
    pub fn register_task(&self, account_id: String, handle: tokio::task::JoinHandle<()>) {
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
fn valid_address(credentials: &Value, key: &str, label: &str) -> Result<String, String> {
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

/// Build a read-only adapter for one stored account. Returns an error
/// when the needed public identifier is missing from the vault record.
/// AsterDEX never errors on missing credentials: the adapter logs the
/// missing-key reason to the console and skips polling.
pub fn build_adapter(
    row: &AccountRow,
    credentials: &Value,
    bus: EventBus,
    live: Arc<RwLock<HashMap<String, AccountLive>>>,
    reconcile: Arc<ReconcileState>,
) -> Result<Arc<dyn ExchangeAdapter>, String> {
    let account_id = row.id.clone();
    match row.exchange_id.as_str() {
        "hyperliquid" | "entropy" => {
            let dex = if row.exchange_id == "entropy" {
                Some("io")
            } else {
                None
            };
            let address = valid_address(credentials, "wallet_address", "主钱包地址")?;
            // The Agent/API wallet private key is optional at connect
            // time: accounts saved before it was required (or saved
            // without one) stay read-only; place_order fails with a
            // clear message instead of a signing error. A present but
            // malformed key fails fast here, like AsterDEX.
            let agent_secret = match credentials
                .get("agent_private_key")
                .and_then(|v| v.as_str())
            {
                Some(k) if !k.trim().is_empty() => Some(
                    crate::adapters::asterdex::decode_agent_secret(k)
                        .map_err(|e| format!("Agent 钱包私钥不正确：{e}"))?,
                ),
                _ => None,
            };
            Ok(Arc::new(HyperliquidAdapter::new(
                &row.exchange_id,
                dex,
                address,
                account_id,
                bus,
                live,
                reconcile,
                agent_secret,
            )))
        }
        "lighter" | "lighter_robinhood" => {
            let network = if row.exchange_id == "lighter_robinhood" {
                LighterNetwork::Robinhood
            } else {
                LighterNetwork::Mainnet
            };
            let index = credentials
                .get("account_index")
                .and_then(|v| v.as_u64())
                .ok_or_else(|| "保存的凭证缺少 Account Index".to_string())?;
            Ok(Arc::new(LighterAdapter::new(
                &row.exchange_id,
                network,
                index,
                account_id,
                bus,
                live,
                reconcile,
            )))
        }
        "asterdex" => {
            // API Key mode: V1 HMAC signing. Agent mode: V3 EIP-712
            // signing with the agent private key; the master wallet
            // address is the V3 `user`, the agent address the `signer`.
            // The secret is decoded and checked against the agent address
            // here, so a mismatched key fails fast with a clear error
            // instead of failing every signed request later.
            let mode = credentials
                .get("mode")
                .and_then(|v| v.as_str())
                .unwrap_or("api");
            if mode == "agent" {
                let user = valid_address(credentials, "master_wallet_address", "主钱包地址")?;
                let signer = valid_address(credentials, "agent_wallet_address", "Agent 钱包地址")?;
                let secret_hex = credentials
                    .get("agent_private_key")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| "保存的凭证缺少 Agent 钱包私钥".to_string())?;
                let secret = crate::adapters::asterdex::decode_agent_secret(secret_hex)?;
                let derived = crate::adapters::asterdex::address_from_secret(&secret);
                if !derived.eq_ignore_ascii_case(&signer) {
                    return Err(
                        "Agent 钱包私钥与 Agent 钱包地址不匹配，请检查保存的凭证".to_string()
                    );
                }
                return Ok(Arc::new(AsterdexAdapter::new_agent(
                    &row.exchange_id,
                    account_id,
                    user,
                    signer,
                    secret,
                    bus,
                    live,
                    reconcile,
                )));
            }
            let (api_key, api_secret) = (
                credentials
                    .get("api_key")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                credentials
                    .get("api_secret")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
            );
            Ok(Arc::new(AsterdexAdapter::new(
                &row.exchange_id,
                account_id,
                api_key,
                api_secret,
                bus,
                live,
                reconcile,
            )))
        }
        "arcus" => {
            let address = valid_address(credentials, "wallet_address", "钱包地址")?;
            Ok(Arc::new(ArcusAdapter::new(
                &row.exchange_id,
                address,
                account_id,
                bus,
                live,
                reconcile,
            )))
        }
        other => Err(format!("本阶段暂不支持 {other} 的只读同步")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::types::LivePosition;

    fn live_with_positions() -> AccountLive {
        AccountLive {
            account_id: "acc-1".to_string(),
            adapter_id: "hyperliquid".to_string(),
            status: "connected".to_string(),
            latency_ms: Some(120),
            equity_usd: 10000.0,
            available_usd: 8000.0,
            pnl_usd: 0.0,
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
}
