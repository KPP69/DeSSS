/// One venue, one directory: the registry every exchange plugs into
/// (v3 §1.1). Adding an exchange is one new directory + one line in
/// [`all()`]; Hyperliquid / Entropy registered in PR-1.2, Lighter in
/// PR-1.3 commit 1, AsterDEX / Arcus in commit 2. Dispatch is
/// registry-only since PR-1.3 commit 3 (no match arms remain).
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;

use crate::bus::EventBus;
use crate::core::error::{VenueError, VenueResult};
use crate::core::market::MarketInfo;
use crate::core::stream_kind::StreamKind;
use crate::core::types::{
    AccountLive, Balance, Order, OrderInfo, OrderRequest, Position, TradeInfo,
};
use crate::market::reconcile::ReconcileState;

pub mod arcus;
pub mod asterdex;
pub mod hyperliquid;
pub mod lighter;

/// One credential field, shared by the frontend form and the backend
/// check (v3 §1.1): a venue declares its fields once, both sides read
/// the same definition.
#[derive(Clone, Copy, Debug)]
pub struct CredentialField {
    /// Key in the credential JSON object (e.g. "wallet_address").
    pub key: &'static str,
    /// Label the frontend form shows for the field.
    pub label: &'static str,
    /// Secret input: masked in the form, never echoed or logged.
    pub secret: bool,
    /// Must be present to connect.
    pub required: bool,
    /// Numeric input (account / API key index): bounds the frontend
    /// form and the backend check share. `None` bounds are open.
    pub kind: CredentialKind,
    pub min: Option<i64>,
    pub max: Option<i64>,
    /// Reserved values inside the range (Lighter RH: 157).
    pub forbidden: &'static [i64],
    /// Short help the form shows under the field, if any.
    pub hint: Option<&'static str>,
}

/// A credential field's input kind: free text or a bounded integer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CredentialKind {
    Text,
    Number,
}

/// Which numeric bound a value broke: range first, then reserved
/// (RH 157 is in range, so it reports `Forbidden`, not a range error).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NumberViolation {
    BelowMin,
    AboveMax,
    Forbidden,
}

/// Check one numeric field value against its declared bounds.
/// Returns the first violation, or `None` when the value is valid.
pub fn check_number_bounds(value: i64, field: &CredentialField) -> Option<NumberViolation> {
    if field.kind != CredentialKind::Number {
        return None;
    }
    if let Some(min) = field.min {
        if value < min {
            return Some(NumberViolation::BelowMin);
        }
    }
    if let Some(max) = field.max {
        if value > max {
            return Some(NumberViolation::AboveMax);
        }
    }
    if field.forbidden.contains(&value) {
        return Some(NumberViolation::Forbidden);
    }
    None
}

/// What `Venue::build` needs beyond the credential JSON: the account
/// being connected and the shared runtime handles. Mirrors the
/// arguments of `SyncManager::build_adapter` today, so the PR-1.2
/// move is a re-route, not a redesign.
#[derive(Clone)]
pub struct VenueCtx {
    pub account_id: String,
    pub bus: EventBus,
    pub live: Arc<RwLock<HashMap<String, AccountLive>>>,
    pub reconcile: Arc<ReconcileState>,
}

/// A market stream's dial plan for one symbol: where to connect, what
/// to subscribe, and which protocol the frames speak (so the engine
/// knows which parser to run). Same shape as `RealtimeEngine`'s
/// internal `Plan`; the engine takes this type over in PR-1.2.
#[derive(Clone, Debug)]
pub struct StreamPlan {
    pub kind: StreamKind,
    pub url: String,
    /// Subscribe message to send after the handshake, if any
    /// (AsterDEX encodes the stream in the URL itself).
    pub subscribe: Option<String>,
}

/// One exchange, behind one interface (v3 §1.1).
#[async_trait]
pub trait Venue: Send + Sync {
    /// Stable id: hyperliquid / entropy / lighter / lighter_robinhood
    /// / asterdex / arcus.
    fn id(&self) -> &'static str;
    /// Display name for the account form and the tables; defaults
    /// to the id, a venue overrides it when the two differ.
    fn display_name(&self) -> &'static str {
        self.id()
    }
    /// One-line note shown under the exchange selector, if any.
    fn note(&self) -> Option<&'static str> {
        None
    }
    /// Credential fields (frontend form and backend check share them).
    fn credentials(&self) -> &'static [CredentialField];
    /// Validate a credential set and derive the masked identifier
    /// stored in SQLite. Error messages name fields, never values.
    fn validate_credentials(
        &self,
        creds: &serde_json::Map<String, serde_json::Value>,
    ) -> Result<String, String>;
    /// Validate the credentials and build a connected account's
    /// runtime handle.
    fn build(
        &self,
        creds: &serde_json::Value,
        ctx: &VenueCtx,
    ) -> Result<Arc<dyn ExchangeAdapter>, String>;
    /// Fetch this venue's market catalog.
    async fn catalog(&self, http: &reqwest::Client) -> Result<Vec<MarketInfo>, String>;
    /// The stream plan for this symbol (URL + subscribe + parser).
    /// Err when the plan cannot be built yet: a missing catalog
    /// market id is transient — the engine retries it (PR-1.3
    /// commit 3 turns it into a typed retry/permanent decision).
    fn stream(&self, symbol: &str, market_id: Option<u64>) -> Result<StreamPlan, String>;
    /// Fill the per-symbol gaps the bulk catalog cannot provide
    /// (true BBO for HL/Entropy, open interest for AsterDEX).
    /// Default: no-op — a venue whose bulk rows are already
    /// complete does nothing.
    async fn enrich(&self, http: &reqwest::Client, row: &mut MarketInfo) -> Result<(), String> {
        let _ = (http, row);
        Ok(())
    }
}

/// Unified exchange contract, implemented by the real read-only adapters
/// (Hyperliquid / Entropy / Lighter). No mock adapter remains: every
/// number the app shows comes from a real exchange read.
#[async_trait]
pub trait ExchangeAdapter: Send + Sync {
    fn id(&self) -> &str;
    fn name(&self) -> &str;
    async fn connect(&self) -> VenueResult<()>;
    async fn disconnect(&self) -> VenueResult<()>;
    async fn subscribe_ticks(&self, symbols: Vec<String>) -> VenueResult<()>;
    async fn unsubscribe_ticks(&self, symbols: Vec<String>) -> VenueResult<()>;
    async fn get_balances(&self, account_id: &str) -> VenueResult<Vec<Balance>>;
    async fn get_positions(&self, account_id: &str) -> VenueResult<Vec<Position>>;
    async fn get_open_orders(&self, account_id: &str) -> VenueResult<Vec<Order>>;
    async fn place_order(&self, req: OrderRequest) -> VenueResult<Order>;
    /// Cancel one order. `order_id` is the exchange oid, or a cloid
    /// (`0x…`); `symbol` is needed to resolve the asset index.
    /// `Ok(())` means the exchange confirmed the cancel. A definite
    /// rejection (already filled / already canceled / never placed)
    /// is `Err(Rejected { .. })`: it proves the cancel was not sent
    /// (v3 §2.1 `pre_send`), so the caller reports it instead of
    /// querying — the record refreshes on the next open-orders poll.
    /// A send whose outcome is unknown is `Err(UnknownOutcome(..))`
    /// — the caller must query as well.
    async fn cancel_order(&self, account_id: &str, order_id: &str, symbol: &str)
        -> VenueResult<()>;
    /// Authoritative order state by cloid (orderStatus + fills).
    /// `Err(Rejected { msg, .. })` with `msg == UNKNOWN_OID` means
    /// the exchange has no such order — the caller keeps the order
    /// Unresolved, it does NOT mean the order was never placed.
    /// Default: the exchange does not support ordering yet.
    async fn query_order_by_cloid(
        &self,
        _account_id: &str,
        _cloid: &str,
    ) -> VenueResult<OrderInfo> {
        Err(VenueError::unsupported("查询订单"))
    }
    /// Authoritative order state by numeric exchange oid
    /// (orderStatus). Same `unknownOid` contract as
    /// `query_order_by_cloid` (a `Rejected` whose `msg` is the
    /// `UNKNOWN_OID` constant). This is a separate method on purpose:
    /// the `oid` field must be posted as a JSON **number** — a
    /// decimal oid sent as a string is rejected by the exchange with
    /// "Failed to deserialize the JSON body into the target type"
    /// (verified live 2026-11-26), so the two id forms must never
    /// share one code path. Default: the exchange does not support
    /// ordering yet.
    async fn query_order_by_oid(&self, _account_id: &str, _oid: u64) -> VenueResult<OrderInfo> {
        Err(VenueError::unsupported("查询订单"))
    }
    /// Fills of one order, by cloid (userFillsByTime matched by oid).
    /// Default: the exchange does not support ordering yet.
    async fn query_fills(&self, _account_id: &str, _cloid: &str) -> VenueResult<Vec<TradeInfo>> {
        Err(VenueError::unsupported("查询成交"))
    }
    /// Long-running read loop (WS + REST polling). Default is a no-op; real
    /// adapters override it. The caller spawns it once after connect().
    async fn run(&self) {}

    /// Wake the poll loop for an immediate account sync (position
    /// reconciliation). Default is a no-op; adapters with a REST loop
    /// override it to notify their loop.
    fn wake_sync(&self) {}
}

/// The registry: adding a venue is one directory + one line here.
/// Hyperliquid / Entropy moved in PR-1.2, Lighter in PR-1.3
/// commit 1, AsterDEX / Arcus in commit 2. All six venues are
/// registered; the old match arms are deleted in commit 3.
pub fn all() -> &'static [&'static dyn Venue] {
    static ALL: [&'static dyn Venue; 6] = [
        &hyperliquid::HYPERLIQUID,
        &hyperliquid::ENTROPY,
        &lighter::LIGHTER,
        &lighter::LIGHTER_ROBINHOOD,
        &asterdex::ASTERDEX,
        &arcus::ARCUS,
    ];
    &ALL
}

/// One credential field as the frontend form sees it (Phase 2, I10):
/// the wire shape of [`CredentialField`]. The form renders exactly
/// this — field order, required flag, number bounds, forbidden
/// values and hints all come from the registry, never from a
/// frontend table.
#[derive(serde::Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct VenueSchemaField {
    pub key: &'static str,
    pub label: &'static str,
    /// "text" | "password" | "number": secret fields are passwords,
    /// numeric fields are numbers, everything else is text.
    pub kind: &'static str,
    pub required: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max: Option<i64>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub forbidden: Vec<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hint: Option<&'static str>,
}

/// One venue as the account form sees it (Phase 2, I10).
#[derive(serde::Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct VenueSchema {
    pub id: &'static str,
    pub name: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<&'static str>,
    pub fields: Vec<VenueSchemaField>,
}

/// The whole form definition, straight from the registry: adding a
/// field to a venue's `credentials()` (or removing one) changes
/// what the frontend renders with zero frontend edits — the mapping
/// below is 1:1, there is no other source.
pub fn venue_schemas() -> Vec<VenueSchema> {
    all()
        .iter()
        .map(|v| VenueSchema {
            id: v.id(),
            name: v.display_name(),
            note: v.note(),
            fields: v
                .credentials()
                .iter()
                .map(|f| VenueSchemaField {
                    key: f.key,
                    label: f.label,
                    kind: if f.secret {
                        "password"
                    } else if f.kind == CredentialKind::Number {
                        "number"
                    } else {
                        "text"
                    },
                    required: f.required,
                    min: f.min,
                    max: f.max,
                    forbidden: f.forbidden.to_vec(),
                    hint: f.hint,
                })
                .collect(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_lists_the_moved_venues() {
        // PR-1.2/1.3: the moved venues are registered; the assembly
        // path iterates all() and dispatches to the registered venue
        // first (the real AppState assembly still needs ./data +
        // SQLite + vault, so it stays a manual check).
        let ids: Vec<&str> = all().iter().map(|v| v.id()).collect();
        assert_eq!(
            ids,
            [
                "hyperliquid",
                "entropy",
                "lighter",
                "lighter_robinhood",
                "asterdex",
                "arcus"
            ]
        );
        let mut visited = 0;
        for venue in all() {
            let _ = venue.credentials();
            visited += 1;
        }
        assert_eq!(visited, 6);
    }

    #[test]
    fn venue_schemas_mirror_the_registry() {
        // Phase 2, I10: the schema is a 1:1 projection of the
        // registry — ids in registry order, every field key present
        // in the same order, nothing invented, nothing dropped. A
        // field added to (or removed from) a venue's credentials()
        // flows to the frontend with no other change.
        let schemas = venue_schemas();
        assert_eq!(schemas.len(), 6);
        for (schema, venue) in schemas.iter().zip(all().iter()) {
            assert_eq!(schema.id, venue.id());
            assert_eq!(schema.name, venue.display_name());
            assert_eq!(schema.note, venue.note());
            let keys: Vec<&str> = schema.fields.iter().map(|f| f.key).collect();
            let want: Vec<&str> = venue.credentials().iter().map(|f| f.key).collect();
            assert_eq!(
                keys, want,
                "schema fields must equal credentials() for {}",
                schema.id
            );
        }
        // Spot checks on the values the form depends on.
        let rh = &schemas[3];
        assert_eq!(rh.id, "lighter_robinhood");
        assert_eq!(rh.name, "Lighter RH");
        let idx = rh.fields.iter().find(|f| f.key == "api_key_index").unwrap();
        assert_eq!(idx.kind, "number");
        assert_eq!(idx.min, Some(4));
        assert_eq!(idx.max, Some(254));
        assert_eq!(idx.forbidden, vec![157]);
        assert!(idx.hint.unwrap().contains("157"));
        let aster = &schemas[4];
        assert_eq!(aster.note, Some("V3 仅支持 Agent 钱包凭证"));
        let wallet = aster
            .fields
            .iter()
            .find(|f| f.key == "wallet_address")
            .unwrap();
        assert_eq!(wallet.kind, "text");
        assert!(wallet.hint.is_some());
        let entropy = &schemas[1];
        assert_eq!(entropy.note, Some("复用 Hyperliquid HIP-3 接入模式"));
        // The wire shape is exactly what the frontend types expect.
        let v = serde_json::to_value(&schemas).unwrap();
        let first = &v[0];
        assert_eq!(first["id"], "hyperliquid");
        assert_eq!(first["name"], "Hyperliquid");
        assert_eq!(first["fields"][1]["kind"], "password");
        assert!(first.get("note").is_none() || first["note"].is_null());
    }
}
