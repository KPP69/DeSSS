//! Lighter / Lighter RH venue (PR-1.3 commit 1): one implementation,
//! two registry entries — mainnet (chain 304, USDC) and Robinhood
//! Chain (chain 466324, USDG) differ only by the `LighterNetwork`
//! parameter, never by a branch on the exchange id.
//!
//! - Market data: none. The adapter opens no WS connection at all —
//!   public market data comes from the on-demand catalog.
//! - Account (read-only): `GET /api/v1/account?by=index&value={index}` —
//!   real collateral, available balance and positions. Unrealised PnL
//!   is summed from the positions: the account object carries no
//!   `unrealized_pnl` field (verified live 2026-11-27 against mainnet
//!   accounts 0-13), so the old account-level read was a constant 0.
//!   When a position lacks the field the sum cannot be trusted and
//!   PnL is `None` (the frontend shows —).
//! - No order placement in this phase.

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

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum LighterNetwork {
    Mainnet,
    Robinhood,
}

impl LighterNetwork {
    pub fn rest_base(&self) -> &'static str {
        match self {
            LighterNetwork::Mainnet => "https://mainnet.zklighter.elliot.ai",
            LighterNetwork::Robinhood => "https://api.rh.lighter.xyz",
        }
    }

    /// Signing chain id (recorded for completeness; the read-only phase
    /// never signs anything).
    pub fn chain_id(&self) -> u64 {
        match self {
            LighterNetwork::Mainnet => 304,
            LighterNetwork::Robinhood => 466324,
        }
    }

    pub fn currency(&self) -> &'static str {
        match self {
            LighterNetwork::Mainnet => "USDC",
            LighterNetwork::Robinhood => "USDG",
        }
    }
}

/// Last REST snapshot, published as `AccountLive`.
#[derive(Clone, Default)]
struct Snapshot {
    equity: f64,
    available: f64,
    /// Unrealised PnL summed from the kept positions; `None` when
    /// a kept position lacks the field (never a fake 0).
    pnl: Option<f64>,
    positions: Vec<LivePosition>,
}

pub struct LighterAdapter {
    id: String,
    network: LighterNetwork,
    account_index: u64,
    account_id: String,
    http: reqwest::Client,
    bus: EventBus,
    live: Arc<RwLock<HashMap<String, AccountLive>>>,
    running: Arc<AtomicBool>,
    /// symbol ("BTC"/"ETH") -> market_id, resolved from /api/v1/orderBooks.
    market_ids: Mutex<HashMap<String, u64>>,
    /// market_id -> symbol, for naming positions that carry only a
    /// market id.
    symbols: Mutex<HashMap<u64, String>>,
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

impl LighterAdapter {
    pub fn new(
        id: &str,
        network: LighterNetwork,
        account_index: u64,
        account_id: String,
        bus: EventBus,
        live: Arc<RwLock<HashMap<String, AccountLive>>>,
        reconcile: Arc<ReconcileState>,
    ) -> Self {
        Self {
            id: id.to_string(),
            network,
            account_index,
            account_id,
            http: crate::net::shared_http_client(),
            bus,
            live,
            running: Arc::new(AtomicBool::new(false)),
            market_ids: Mutex::new(HashMap::new()),
            symbols: Mutex::new(HashMap::new()),
            snapshot: Mutex::new(Snapshot::default()),
            rest_ok: AtomicBool::new(false),
            last_error: Mutex::new(None),
            last_rest_rtt: Mutex::new(None),
            reconcile,
            wake: Arc::new(tokio::sync::Notify::new()),
        }
    }
}

/// One Lighter chain behind one implementation: the two registry
/// entries differ only by the `network` parameter, never by a branch
/// on the exchange id.
pub struct LighterVenue {
    id: &'static str,
    network: LighterNetwork,
}

pub static LIGHTER: LighterVenue = LighterVenue {
    id: "lighter",
    network: LighterNetwork::Mainnet,
};
pub static LIGHTER_ROBINHOOD: LighterVenue = LighterVenue {
    id: "lighter_robinhood",
    network: LighterNetwork::Robinhood,
};

const LIGHTER_FIELDS: &[CredentialField] = &[
    CredentialField {
        key: "account_index",
        label: "Account Index",
        secret: false,
        required: true,
        kind: CredentialKind::Number,
        min: Some(0),
        max: None,
        forbidden: &[],
        hint: None,
    },
    CredentialField {
        key: "api_key_index",
        label: "API Key Index",
        secret: false,
        required: true,
        kind: CredentialKind::Number,
        min: Some(4),
        max: Some(254),
        forbidden: &[],
        hint: Some("允许范围 4–254"),
    },
    CredentialField {
        key: "api_key_private_key",
        label: "API Key 私钥",
        secret: true,
        required: true,
        kind: CredentialKind::Text,
        min: None,
        max: None,
        forbidden: &[],
        hint: None,
    },
];

const RH_FIELDS: &[CredentialField] = &[
    CredentialField {
        key: "account_index",
        label: "Account Index",
        secret: false,
        required: true,
        kind: CredentialKind::Number,
        min: Some(0),
        max: None,
        forbidden: &[],
        hint: None,
    },
    CredentialField {
        key: "api_key_index",
        label: "API Key Index",
        secret: false,
        required: true,
        kind: CredentialKind::Number,
        min: Some(4),
        max: Some(254),
        forbidden: &[157],
        hint: Some("允许范围 4–156 或 158–254，请避开保留位 157"),
    },
    CredentialField {
        key: "api_key_private_key",
        label: "API Key 私钥",
        secret: true,
        required: true,
        kind: CredentialKind::Text,
        min: None,
        max: None,
        forbidden: &[],
        hint: None,
    },
];

#[async_trait]
impl Venue for LighterVenue {
    fn id(&self) -> &'static str {
        self.id
    }

    fn display_name(&self) -> &'static str {
        match self.network {
            LighterNetwork::Mainnet => "Lighter",
            LighterNetwork::Robinhood => "Lighter RH",
        }
    }

    fn credentials(&self) -> &'static [CredentialField] {
        match self.network {
            LighterNetwork::Mainnet => LIGHTER_FIELDS,
            LighterNetwork::Robinhood => RH_FIELDS,
        }
    }

    fn validate_credentials(
        &self,
        creds: &serde_json::Map<String, serde_json::Value>,
    ) -> Result<String, String> {
        use crate::venues::NumberViolation;
        let fields = self.credentials();
        let account_index =
            crate::accounts::require_index(creds, "account_index", "Account Index")?;
        if let Some(v) = crate::venues::check_number_bounds(account_index, &fields[0]) {
            return Err(match v {
                NumberViolation::BelowMin => "Account Index 不能为负数".to_string(),
                _ => "Account Index 无效".to_string(),
            });
        }
        let api_key_index =
            crate::accounts::require_index(creds, "api_key_index", "API Key Index")?;
        if let Some(v) = crate::venues::check_number_bounds(api_key_index, &fields[1]) {
            return Err(match v {
                NumberViolation::BelowMin | NumberViolation::AboveMax => {
                    if self.network == LighterNetwork::Robinhood {
                        "Lighter RH API Key Index 须为 4–156 或 158–254".to_string()
                    } else {
                        "Lighter API Key Index 须在 4–254 之间".to_string()
                    }
                }
                NumberViolation::Forbidden => {
                    "Lighter RH 的 API Key Index 157 已保留，不可用".to_string()
                }
            });
        }
        crate::accounts::require_str(creds, "api_key_private_key", "API Key 私钥")?;
        Ok(format!(
            "Account #{account_index} · API key #{api_key_index}"
        ))
    }

    fn build(
        &self,
        creds: &serde_json::Value,
        ctx: &VenueCtx,
    ) -> Result<Arc<dyn ExchangeAdapter>, String> {
        let index = creds
            .get("account_index")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| "保存的凭证缺少 Account Index".to_string())?;
        Ok(Arc::new(LighterAdapter::new(
            self.id,
            self.network,
            index,
            ctx.account_id.clone(),
            ctx.bus.clone(),
            ctx.live.clone(),
            ctx.reconcile.clone(),
        )))
    }

    async fn catalog(&self, http: &reqwest::Client) -> Result<Vec<MarketInfo>, String> {
        catalog::fetch_lighter(http, self.network).await
    }

    async fn fetch_quotes(
        &self,
        http: &reqwest::Client,
    ) -> Result<Vec<crate::core::market::QuoteUpdate>, String> {
        // The batch quote endpoints are the same orderBookDetails +
        // funding-rates responses as the bulk fetch (mark_price IS in
        // orderBookDetails — not last_trade_price). Map the rows.
        let rows = catalog::fetch_lighter(http, self.network).await?;
        Ok(rows.iter().map(|r| r.to_quote_update()).collect())
    }

    fn stream(&self, symbol: &str, market_id: Option<u64>) -> Result<StreamPlan, String> {
        stream::stream_plan(symbol, self.network, market_id)
    }

    async fn enrich(&self, http: &reqwest::Client, row: &mut MarketInfo) -> Result<(), String> {
        let Some(mid) = row.market_id else {
            return Ok(());
        };
        let (bid, ask) = catalog::fetch_lighter_book_orders(http, self.network, mid).await?;
        if bid.is_some() {
            row.bid_px = bid;
        }
        if ask.is_some() {
            row.ask_px = ask;
        }
        // Bulk mid was last-trade price; the true mid from the filled
        // BBO is better.
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
    fn validate_enforces_numeric_bounds() {
        let base = |idx: i64| {
            creds(serde_json::json!({
                "account_index": 0,
                "api_key_index": idx,
                "api_key_private_key": "0xkey"
            }))
        };
        // Mainnet: 4-254.
        assert!(LIGHTER.validate_credentials(&base(3)).is_err());
        assert!(LIGHTER.validate_credentials(&base(255)).is_err());
        assert!(LIGHTER.validate_credentials(&base(4)).is_ok());
        assert!(LIGHTER.validate_credentials(&base(254)).is_ok());
        // Robinhood: 157 is reserved inside the range; 156/158 pass.
        let err = LIGHTER_ROBINHOOD
            .validate_credentials(&base(157))
            .expect_err("157 reserved");
        assert!(err.contains("157"), "{err}");
        assert!(LIGHTER_ROBINHOOD.validate_credentials(&base(156)).is_ok());
        assert!(LIGHTER_ROBINHOOD.validate_credentials(&base(158)).is_ok());
        // A negative account index fails early.
        let neg = creds(serde_json::json!({
            "account_index": -1,
            "api_key_index": 4,
            "api_key_private_key": "0xkey"
        }));
        assert!(LIGHTER.validate_credentials(&neg).is_err());
    }
}
