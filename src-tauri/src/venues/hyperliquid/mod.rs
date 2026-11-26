//! Hyperliquid / Entropy venue (PR-1.2): one implementation, two
//! registry entries — the mainnet perp dex and the HIP-3 `io` dex
//! differ only by the dex parameter, never by a branch on the
//! exchange id.
//!
//! - Market data: none. The adapter opens no WS connection at all —
//!   public market data comes from the on-demand catalog.
//! - Account (read-only): `POST https://api.hyperliquid.xyz/info`
//!   `clearinghouseState` — real equity, withdrawable margin, open
//!   positions and unrealised PnL.
//! - Orders (phase 5A §4): real limit / post-only / IOC orders via
//!   `POST /exchange` with pure-Rust EIP-712 L1 signing
//!   (`adapters/hl_signing.rs`, golden-vector tested against the
//!   official Python SDK). The signing key is the account's
//!   Agent/API wallet private key, decoded at connect time; accounts
//!   without one stay read-only. Orders are one-shot sends — a
//!   write is NEVER retried, and a send with no usable answer
//!   returns `UnknownOutcome`, never a guessed fill.
//! - Unified account: one master wallet shares a single pool across
//!   mainnet perp (`dex = ""`), Entropy (`dex = "io"`) and spot. The
//!   account read always aggregates all three, so the Hyperliquid and
//!   Entropy adapters publish identical numbers for the same wallet.

pub mod catalog;
pub mod order;
pub mod rest;
pub mod sign;
pub mod stream;

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;

use crate::adapters::market_catalog::MarketInfo;
use crate::adapters::reconcile::ReconcileState;
use crate::adapters::ExchangeAdapter;
use crate::bus::EventBus;
use crate::core::types::{AccountLive, LivePosition};
use crate::venues::{CredentialField, StreamPlan, Venue, VenueCtx};

pub const HL_INFO_URL: &str = "https://api.hyperliquid.xyz/info";
pub const HL_EXCHANGE_URL: &str = "https://api.hyperliquid.xyz/exchange";

/// Asset-index cache TTL: the meta universe changes only when the
/// exchange lists/delists a market.
const ASSET_META_TTL: Duration = Duration::from_secs(300);

/// One-shot write timeout: an order send that hangs longer than this
/// is an unknown outcome, not a failure — the caller reconciles.
const WRITE_TIMEOUT: Duration = Duration::from_secs(30);

const POLL_INTERVAL: Duration = Duration::from_secs(10);

/// Last REST snapshot, published as `AccountLive`.
#[derive(Clone, Default)]
pub(crate) struct Snapshot {
    equity_usd: f64,
    available_usd: f64,
    pnl_usd: f64,
    positions: Vec<LivePosition>,
}

pub struct HyperliquidAdapter {
    id: String,
    dex: Option<String>,
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
    /// Agent/API wallet private key for order signing, when the
    /// account saved one. Never logged, never returned, never sent
    /// anywhere except into the EIP-712 signature. `None` means the
    /// account is read-only: reads work, orders fail with a clear
    /// message.
    agent_secret: Option<[u8; 32]>,
    /// Address the agent key signs as (derived from the key).
    agent_address: Option<String>,
    /// Cached asset-index universe for order placement (meta +
    /// perpDexs offset for HIP-3). Refreshed lazily, 5 min TTL.
    asset_meta: Mutex<Option<AssetMeta>>,
}

/// Cached `meta` universe for one dex: symbol order IS the asset
/// index (main dex offset 0; HIP-3 dexes start at 110000, +10000 per
/// further dex — the SDK's `perp_dex_to_offset` rule).
#[derive(Clone)]
struct AssetMeta {
    fetched_at: Instant,
    universe: Vec<String>,
    dex_offset: u32,
}

impl HyperliquidAdapter {
    pub fn new(
        id: &str,
        dex: Option<&str>,
        address: String,
        account_id: String,
        bus: EventBus,
        live: Arc<RwLock<HashMap<String, AccountLive>>>,
        reconcile: Arc<ReconcileState>,
        agent_secret: Option<[u8; 32]>,
    ) -> Self {
        let agent_address = agent_secret
            .as_ref()
            .map(|s| crate::adapters::asterdex::address_from_secret(s));
        Self {
            id: id.to_string(),
            dex: dex.map(|d| d.to_string()),
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
            agent_secret,
            agent_address,
            asset_meta: Mutex::new(None),
        }
    }

    /// The dex this adapter trades on: `""` for mainnet perp,
    /// `"io"` for Entropy.
    pub(crate) fn dex_name(&self) -> &str {
        self.dex.as_deref().unwrap_or("")
    }
}

/// One venue entry: the id and the dex parameter are data, not
/// branches — Hyperliquid and Entropy share this implementation
/// exactly, and no code here tests which entry it is.
pub struct HyperliquidVenue {
    id: &'static str,
    dex: Option<&'static str>,
}

pub static HYPERLIQUID: HyperliquidVenue = HyperliquidVenue {
    id: "hyperliquid",
    dex: None,
};
pub static ENTROPY: HyperliquidVenue = HyperliquidVenue {
    id: "entropy",
    dex: Some("io"),
};

#[async_trait]
impl Venue for HyperliquidVenue {
    fn id(&self) -> &'static str {
        self.id
    }

    fn credentials(&self) -> &'static [CredentialField] {
        &[
            CredentialField {
                key: "wallet_address",
                label: "主钱包地址",
                secret: false,
                required: true,
            },
            CredentialField {
                key: "agent_private_key",
                label: "Agent/API 钱包私钥",
                secret: true,
                required: true,
            },
        ]
    }

    fn validate_credentials(
        &self,
        creds: &serde_json::Map<String, serde_json::Value>,
    ) -> Result<String, String> {
        // Entropy reuses the Hyperliquid HIP-3 access pattern: same fields.
        let wallet = crate::accounts::require_str(creds, "wallet_address", "主钱包地址")?;
        crate::accounts::require_str(creds, "agent_private_key", "Agent/API 钱包私钥")?;
        Ok(crate::accounts::mask_address(&wallet))
    }

    fn build(
        &self,
        creds: &serde_json::Value,
        ctx: &VenueCtx,
    ) -> Result<Arc<dyn ExchangeAdapter>, String> {
        let address = crate::adapters::sync::valid_address(creds, "wallet_address", "主钱包地址")?;
        // The Agent/API wallet private key is optional at connect
        // time: accounts saved before it was required (or saved
        // without one) stay read-only; place_order fails with a
        // clear message instead of a signing error. A present but
        // malformed key fails fast here, like AsterDEX.
        let agent_secret = match creds.get("agent_private_key").and_then(|v| v.as_str()) {
            Some(k) if !k.trim().is_empty() => Some(
                crate::adapters::asterdex::decode_agent_secret(k)
                    .map_err(|e| format!("Agent 钱包私钥不正确：{e}"))?,
            ),
            _ => None,
        };
        Ok(Arc::new(HyperliquidAdapter::new(
            self.id,
            self.dex,
            address,
            ctx.account_id.clone(),
            ctx.bus.clone(),
            ctx.live.clone(),
            ctx.reconcile.clone(),
            agent_secret,
        )))
    }

    async fn catalog(&self, http: &reqwest::Client) -> Result<Vec<MarketInfo>, String> {
        catalog::fetch_hyperliquid(http, self.dex).await
    }

    fn stream(&self, symbol: &str, _market_id: Option<u64>) -> StreamPlan {
        stream::stream_plan(symbol)
    }

    async fn enrich(&self, http: &reqwest::Client, row: &mut MarketInfo) -> Result<(), String> {
        let (bid, ask) = catalog::fetch_l2book(http, &row.symbol).await?;
        if bid.is_some() {
            row.bid_px = bid;
        }
        if ask.is_some() {
            row.ask_px = ask;
        }
        Ok(())
    }
}
