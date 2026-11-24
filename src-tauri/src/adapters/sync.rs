//! Per-account real-exchange sync manager (phase 3).
//!
//! Owns the running adapters (one per connected account) and the latest
//! live snapshot map the frontend reads through `list_account_live`.
//! Adapters are built from the account row + vault credentials — only the
//! public identifiers are used (wallet address for Hyperliquid/Entropy,
//! account index for Lighter). Private keys are never read here and never
//! leave the vault.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use serde_json::Value;

use super::hyperliquid::HyperliquidAdapter;
use super::lighter::{LighterAdapter, LighterNetwork};
use super::types::AccountLive;
use super::ExchangeAdapter;
use crate::bus::EventBus;
use crate::data_engine::AccountRow;

pub struct SyncManager {
    live: Arc<RwLock<HashMap<String, AccountLive>>>,
    running: Mutex<HashMap<String, Arc<dyn ExchangeAdapter>>>,
}

impl SyncManager {
    pub fn new() -> Self {
        Self { live: Arc::new(RwLock::new(HashMap::new())), running: Mutex::new(HashMap::new()) }
    }

    pub fn live_map(&self) -> Arc<RwLock<HashMap<String, AccountLive>>> {
        self.live.clone()
    }

    pub fn live_states(&self) -> Vec<AccountLive> {
        self.live.read().map(|m| m.values().cloned().collect()).unwrap_or_default()
    }

    pub fn is_running(&self, account_id: &str) -> bool {
        self.running.lock().map(|m| m.contains_key(account_id)).unwrap_or(false)
    }

    pub fn insert(&self, account_id: String, adapter: Arc<dyn ExchangeAdapter>) {
        if let Ok(mut m) = self.running.lock() {
            m.insert(account_id, adapter);
        }
    }

    pub fn remove(&self, account_id: &str) -> Option<Arc<dyn ExchangeAdapter>> {
        self.running.lock().ok().and_then(|mut m| m.remove(account_id))
    }

    pub fn remove_live(&self, account_id: &str) {
        if let Ok(mut m) = self.live.write() {
            m.remove(account_id);
        }
    }
}

/// Build a read-only adapter for one stored account. Returns an error for
/// exchanges that have no read-only adapter in this phase (AsterDEX, Arcus)
/// and when the needed public identifier is missing from the vault record.
pub fn build_adapter(
    row: &AccountRow,
    credentials: &Value,
    bus: EventBus,
    live: Arc<RwLock<HashMap<String, AccountLive>>>,
) -> Result<Arc<dyn ExchangeAdapter>, String> {
    let account_id = row.id.clone();
    match row.exchange_id.as_str() {
        "hyperliquid" | "entropy" => {
            let dex = if row.exchange_id == "entropy" { Some("io") } else { None };
            let address = credentials
                .get("wallet_address")
                .and_then(|v| v.as_str())
                .ok_or_else(|| "wallet_address is missing from the stored credentials".to_string())?
                .to_string();
            if !address.starts_with("0x") || address.len() != 42 {
                return Err("stored wallet_address is not a valid 0x address".to_string());
            }
            Ok(Arc::new(HyperliquidAdapter::new(
                &row.exchange_id, dex, address, account_id, bus, live,
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
                .ok_or_else(|| "account_index is missing from the stored credentials".to_string())?;
            Ok(Arc::new(LighterAdapter::new(
                &row.exchange_id, network, index, account_id, bus, live,
            )))
        }
        other => Err(format!("read-only sync is not implemented for {other} in this phase")),
    }
}
