//! AsterDEX read-only adapter.
//!
//! Official endpoints (asterdex/api-docs, V3 recommended):
//! - Market data: none. The adapter opens no WS connection at all —
//!   public market data comes from the on-demand catalog.
//! - V3 只支持 Agent 钱包（EIP-712）；V1 已于 2026-10-10 退役，代码已移除。
//! - Account, Agent-wallet mode (V3 EIP-712): `GET /fapi/v3/account` +
//!   `/fapi/v3/positionRisk`, signed by EIP-712 over the URL-encoded
//!   parameter string (`user` = master wallet, `signer` = agent address,
//!   `nonce` = microsecond timestamp, `signature` appended last; domain
//!   AsterSignTransaction / 1 / chainId 1666). Pure Rust (`k256` + `sha3`),
//!   no OpenSSL anywhere. No API-key header in V3.
//! - Multi-asset margin: the `total*` fields are USDT-only, so equity /
//!   available / PnL are summed from `assets[]` (marginBalance /
//!   availableBalance / unrealizedProfit) with the totals as fallback;
//!   the displayed settlement currency is the asset with the largest
//!   margin balance (USDT, USDC, USDF, USD1, ...).
//! - No credentials at all: build fails on the missing field, so no
//!   adapter is constructed and the account stays unconnected.

//! Venue home (PR-1.3 commit 2): the adapter moved verbatim from
//! `adapters/asterdex.rs`; the V3 catalog moved from
//! `adapters/market_catalog.rs`. One registry entry, agent wallets
//! only — see the module doc above.

pub mod catalog;
pub mod order;
pub mod rest;
pub mod sign;
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

/// V3 REST base (fapi). Also used by the catalog (exchangeInfo and
/// the four live endpoints).
pub const ASTER_REST_URL: &str = "https://fapi.asterdex.com";

/// V3 Agent-wallet credentials: `user` is the master wallet, `signer`
/// the agent address, `secret` the agent private key (32 bytes).
struct AgentCreds {
    user: String,
    signer: String,
    secret: [u8; 32],
}

/// Last REST snapshot, published as `AccountLive`.
#[derive(Clone, Default)]
struct Snapshot {
    equity_usd: f64,
    available_usd: f64,
    pnl_usd: f64,
    currency: String,
    positions: Vec<LivePosition>,
}

pub struct AsterdexAdapter {
    id: String,
    account_id: String,
    /// V3 Agent-wallet credentials. Always present: build fails
    /// without them, so no read path ever runs credential-less.
    agent: AgentCreds,
    http: reqwest::Client,
    bus: EventBus,
    live: Arc<RwLock<HashMap<String, AccountLive>>>,
    running: Arc<AtomicBool>,
    snapshot: Mutex<Snapshot>,
    rest_ok: AtomicBool,
    last_error: Mutex<Option<String>>,
    last_rest_rtt: Mutex<Option<Duration>>,
    /// Last V3 nonce issued (microseconds, strictly increasing).
    last_nonce: Mutex<u128>,
    /// Post-fill reconciliation grace (spec §3): polls skip their write
    /// while the account is inside the window.
    reconcile: Arc<ReconcileState>,
    /// Woken by `wake_sync` for an immediate out-of-cycle poll.
    wake: Arc<tokio::sync::Notify>,
}

impl AsterdexAdapter {
    /// Agent-wallet mode (V3 EIP-712). The secret was already decoded
    /// and checked against `signer` by the sync manager.
    pub fn new_agent(
        id: &str,
        account_id: String,
        user: String,
        signer: String,
        secret: [u8; 32],
        bus: EventBus,
        live: Arc<RwLock<HashMap<String, AccountLive>>>,
        reconcile: Arc<ReconcileState>,
    ) -> Self {
        Self {
            id: id.to_string(),
            account_id,
            agent: AgentCreds {
                user,
                signer,
                secret,
            },
            http: crate::net::shared_http_client(),
            bus,
            live,
            running: Arc::new(AtomicBool::new(false)),
            snapshot: Mutex::new(Snapshot {
                currency: "USDT".to_string(),
                ..Snapshot::default()
            }),
            rest_ok: AtomicBool::new(false),
            last_error: Mutex::new(None),
            last_rest_rtt: Mutex::new(None),
            last_nonce: Mutex::new(0),
            reconcile,
            wake: Arc::new(tokio::sync::Notify::new()),
        }
    }
}

/// AsterDEX venue: one registry entry, V3 agent wallets only.
/// No order placement in this phase.
pub struct AsterdexVenue;

pub static ASTERDEX: AsterdexVenue = AsterdexVenue;

const ASTERDEX_FIELDS: &[CredentialField] = &[
    CredentialField {
        key: "wallet_address",
        label: "主钱包地址",
        secret: false,
        required: true,
        kind: CredentialKind::Text,
        min: None,
        max: None,
        forbidden: &[],
        hint: Some("拥有该账户、授权了 Agent 的主钱包地址（V3 签名 user 参数）"),
    },
    CredentialField {
        key: "agent_private_key",
        label: "Agent 钱包私钥",
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
impl Venue for AsterdexVenue {
    fn id(&self) -> &'static str {
        "asterdex"
    }

    fn display_name(&self) -> &'static str {
        "AsterDEX"
    }

    fn note(&self) -> Option<&'static str> {
        Some("V3 仅支持 Agent 钱包凭证")
    }

    fn credentials(&self) -> &'static [CredentialField] {
        ASTERDEX_FIELDS
    }

    fn validate_credentials(
        &self,
        creds: &serde_json::Map<String, serde_json::Value>,
    ) -> Result<String, String> {
        let wallet = crate::accounts::require_str(creds, "wallet_address", "主钱包地址")?;
        crate::accounts::require_str(creds, "agent_private_key", "Agent 钱包私钥")?;
        Ok(crate::accounts::mask_address(&wallet))
    }

    fn build(
        &self,
        creds: &serde_json::Value,
        ctx: &VenueCtx,
    ) -> Result<Arc<dyn ExchangeAdapter>, String> {
        let user = crate::market::sync::valid_address(creds, "wallet_address", "主钱包地址")?;
        let secret_hex = creds
            .get("agent_private_key")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "保存的凭证缺少 Agent 钱包私钥".to_string())?;
        let secret = sign::decode_agent_secret(secret_hex)?;
        // The agent (signer) address is derived from the key.
        let signer = sign::address_from_secret(&secret);
        Ok(Arc::new(AsterdexAdapter::new_agent(
            self.id(),
            ctx.account_id.clone(),
            user,
            signer,
            secret,
            ctx.bus.clone(),
            ctx.live.clone(),
            ctx.reconcile.clone(),
        )))
    }

    async fn catalog(&self, http: &reqwest::Client) -> Result<Vec<MarketInfo>, String> {
        catalog::fetch_asterdex(http).await
    }

    fn stream(&self, symbol: &str, _market_id: Option<u64>) -> Result<StreamPlan, String> {
        Ok(stream::stream_plan(symbol))
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
    fn validate_accepts_agent_fields() {
        let agent = creds(serde_json::json!({
            "wallet_address": "0x1111111111111111111111111111111111111111",
            "agent_private_key": "0xkey"
        }));
        assert!(ASTERDEX.validate_credentials(&agent).is_ok());
        let missing = creds(serde_json::json!({ "agent_private_key": "0xkey" }));
        assert!(ASTERDEX.validate_credentials(&missing).is_err());
    }
}
