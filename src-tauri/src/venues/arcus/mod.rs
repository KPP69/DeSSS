//! Arcus read-only adapter.
//!
//! Official endpoints (docs.arcus.xyz/api-reference, updated 2026-11-12):
//! - Market list: `GET https://api.arcus.xyz/v1/markets` — used by the
//!   on-demand catalog (bulk specs + prices + funding).
//! - Market data: none. The adapter opens no WS connection at all —
//!   public market data comes from the on-demand catalog.
//! - Account (read-only, no auth): `GET /v1/account?address=0x…`
//!   `&accountIndex=0` — real equity, free collateral and positions.
//!   There is no total-PnL field: PnL is derived as
//!   `equity - netDeposits` (all-time PnL, same basis as /portfolio).
//!   Settlement asset is USDG. The account form has no subaccount field,
//!   so only index 0 is reachable. The signing key is never read here:
//!   account reads need only the wallet address.

//! Venue home (PR-1.3 commit 2): the adapter moved verbatim from
//! `adapters/arcus.rs`; the catalog moved from
//! `adapters/market_catalog.rs`. One registry entry, read-only.

pub mod catalog;
pub mod rest;
pub mod stream;

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use async_trait::async_trait;

use crate::bus::EventBus;
use crate::core::market::MarketInfo;
use crate::core::types::{AccountLive, LivePosition};
use crate::market::reconcile::ReconcileState;
use crate::venues::ExchangeAdapter;
use crate::venues::{CredentialField, CredentialKind, StreamPlan, Venue, VenueCtx};

/// V3 REST base (v1). Also used by the catalog (/v1/markets, /v1/bbo).
pub const ARCUS_REST_URL: &str = "https://api.arcus.xyz";

/// Last REST snapshot, published as `AccountLive`.
#[derive(Clone, Default)]
struct Snapshot {
    equity_usd: f64,
    available_usd: f64,
    pnl_usd: f64,
    positions: Vec<LivePosition>,
}

pub struct ArcusAdapter {
    id: String,
    address: String,
    account_id: String,
    http: reqwest::Client,
    bus: EventBus,
    live: Arc<RwLock<HashMap<String, AccountLive>>>,
    running: Arc<AtomicBool>,
    snapshot: Mutex<Snapshot>,
    rest_ok: AtomicBool,
    last_error: Mutex<Option<String>>,
    last_rest_rtt: Mutex<Option<Duration>>,
    /// Post-fill reconciliation grace (spec §3): polls skip their write
    /// while the account is inside the window.
    reconcile: Arc<ReconcileState>,
    /// Woken by `wake_sync` for an immediate out-of-cycle poll.
    wake: Arc<tokio::sync::Notify>,
}

impl ArcusAdapter {
    pub fn new(
        id: &str,
        address: String,
        account_id: String,
        bus: EventBus,
        live: Arc<RwLock<HashMap<String, AccountLive>>>,
        reconcile: Arc<ReconcileState>,
    ) -> Self {
        Self {
            id: id.to_string(),
            address,
            account_id,
            http: crate::net::shared_http_client(),
            bus,
            live,
            running: Arc::new(AtomicBool::new(false)),
            snapshot: Mutex::new(Snapshot::default()),
            rest_ok: AtomicBool::new(false),
            last_error: Mutex::new(None),
            last_rest_rtt: Mutex::new(None),
            reconcile,
            wake: Arc::new(tokio::sync::Notify::new()),
        }
    }
}

/// Arcus venue: one registry entry, read-only. No order placement
/// in this phase.
pub struct ArcusVenue;

pub static ARCUS: ArcusVenue = ArcusVenue;

const ARCUS_FIELDS: &[CredentialField] = &[CredentialField {
    key: "wallet_address",
    label: "钱包地址",
    secret: false,
    required: true,
    kind: CredentialKind::Text,
    min: None,
    max: None,
    forbidden: &[],
    hint: None,
}];

#[async_trait]
impl Venue for ArcusVenue {
    fn id(&self) -> &'static str {
        "arcus"
    }

    fn display_name(&self) -> &'static str {
        "Arcus"
    }

    fn credentials(&self) -> &'static [CredentialField] {
        ARCUS_FIELDS
    }

    fn validate_credentials(
        &self,
        creds: &serde_json::Map<String, serde_json::Value>,
    ) -> Result<String, String> {
        let wallet = crate::accounts::require_str(creds, "wallet_address", "钱包地址")?;
        Ok(crate::accounts::mask_address(&wallet))
    }

    fn build(
        &self,
        creds: &serde_json::Value,
        ctx: &VenueCtx,
    ) -> Result<Arc<dyn ExchangeAdapter>, String> {
        let address = crate::market::sync::valid_address(creds, "wallet_address", "钱包地址")?;
        Ok(Arc::new(ArcusAdapter::new(
            self.id(),
            address,
            ctx.account_id.clone(),
            ctx.bus.clone(),
            ctx.live.clone(),
            ctx.reconcile.clone(),
        )))
    }

    async fn catalog(&self, http: &reqwest::Client) -> Result<Vec<MarketInfo>, String> {
        catalog::fetch_arcus(http).await
    }

    fn stream(&self, symbol: &str, _market_id: Option<u64>) -> Result<StreamPlan, String> {
        Ok(stream::stream_plan(symbol))
    }

    async fn enrich(&self, http: &reqwest::Client, row: &mut MarketInfo) -> Result<(), String> {
        let (bid, ask) = catalog::fetch_arcus_bbo(http, &row.symbol).await?;
        if bid.is_some() {
            row.bid_px = bid;
        }
        if ask.is_some() {
            row.ask_px = ask;
        }
        if let (Some(b), Some(a)) = (row.bid_px, row.ask_px) {
            row.mid_px = Some((b + a) / 2.0);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::venues::Venue;

    fn creds(v: serde_json::Value) -> serde_json::Map<String, serde_json::Value> {
        v.as_object().expect("object").clone()
    }

    #[test]
    fn validate_needs_only_the_wallet_address() {
        let ok = creds(serde_json::json!({
            "wallet_address": "0x1111111111111111111111111111111111111111"
        }));
        assert!(ARCUS.validate_credentials(&ok).is_ok());
        let missing = creds(serde_json::json!({}));
        assert!(ARCUS.validate_credentials(&missing).is_err());
    }
}
