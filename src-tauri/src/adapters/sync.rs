//! Per-account real-exchange sync manager (phase 3).
//!
//! Owns the running adapters (one per connected account) and the latest
//! live snapshot map the frontend reads through `list_account_live`.
//! Adapters are built from the account row + vault credentials — only the
//! public identifiers are used (wallet address for Hyperliquid/Entropy,
//! account index for Lighter, wallet address for Arcus). Private keys are
//! never read here and never leave the vault. AsterDEX is the exception
//! that needs a secret: its API Key/Secret pair is passed to the adapter
//! for HMAC signing only, and never logged.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use serde_json::Value;

use super::arcus::ArcusAdapter;
use super::asterdex::AsterdexAdapter;
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
        Self {
            live: Arc::new(RwLock::new(HashMap::new())),
            running: Mutex::new(HashMap::new()),
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

    pub fn remove_live(&self, account_id: &str) {
        if let Ok(mut m) = self.live.write() {
            m.remove(account_id);
        }
    }
}

/// Build a read-only adapter for one stored account. Returns an error
/// when the needed public identifier is missing from the vault record.
/// AsterDEX never errors on missing credentials: the adapter starts the
/// public WS book anyway and logs the missing-key reason to the console.
pub fn build_adapter(
    row: &AccountRow,
    credentials: &Value,
    bus: EventBus,
    live: Arc<RwLock<HashMap<String, AccountLive>>>,
) -> Result<Arc<dyn ExchangeAdapter>, String> {
    let account_id = row.id.clone();
    match row.exchange_id.as_str() {
        "hyperliquid" | "entropy" => {
            let dex = if row.exchange_id == "entropy" {
                Some("io")
            } else {
                None
            };
            let address = credentials
                .get("wallet_address")
                .and_then(|v| v.as_str())
                .ok_or_else(|| "wallet_address is missing from the stored credentials".to_string())?
                .to_string();
            if !address.starts_with("0x") || address.len() != 42 {
                return Err("stored wallet_address is not a valid 0x address".to_string());
            }
            Ok(Arc::new(HyperliquidAdapter::new(
                &row.exchange_id,
                dex,
                address,
                account_id,
                bus,
                live,
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
                .ok_or_else(|| {
                    "account_index is missing from the stored credentials".to_string()
                })?;
            Ok(Arc::new(LighterAdapter::new(
                &row.exchange_id,
                network,
                index,
                account_id,
                bus,
                live,
            )))
        }
        "asterdex" => {
            // The Agent-wallet mode needs V3 EIP-712 auth (unsupported in
            // this phase); the adapter logs that and runs the public WS
            // book only. Same for a missing key pair.
            let mode = credentials
                .get("mode")
                .and_then(|v| v.as_str())
                .unwrap_or("api");
            let (api_key, api_secret) = if mode == "agent" {
                (None, None)
            } else {
                (
                    credentials
                        .get("api_key")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    credentials
                        .get("api_secret")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                )
            };
            Ok(Arc::new(AsterdexAdapter::new(
                &row.exchange_id,
                account_id,
                api_key,
                api_secret,
                bus,
                live,
            )))
        }
        "arcus" => {
            let address = credentials
                .get("wallet_address")
                .and_then(|v| v.as_str())
                .ok_or_else(|| "wallet_address is missing from the stored credentials".to_string())?
                .to_string();
            if !address.starts_with("0x") || address.len() != 42 {
                return Err("stored wallet_address is not a valid 0x address".to_string());
            }
            Ok(Arc::new(ArcusAdapter::new(
                &row.exchange_id,
                address,
                account_id,
                bus,
                live,
            )))
        }
        other => Err(format!(
            "read-only sync is not implemented for {other} in this phase"
        )),
    }
}
