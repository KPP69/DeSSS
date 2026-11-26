//! One venue, one directory: the registry every exchange plugs into
//! (v3 §1.1). Adding an exchange is one new directory + one line in
//! [`all()`]; Hyperliquid / Entropy registered in PR-1.2; the
//! assembly loop dispatches to a registered venue first and falls
//! back to the old match.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;

use crate::adapters::market_catalog::MarketInfo;
use crate::adapters::reconcile::ReconcileState;
use crate::adapters::ExchangeAdapter;
use crate::bus::EventBus;
use crate::core::stream_kind::StreamKind;
use crate::core::types::AccountLive;

pub mod hyperliquid;

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
    fn stream(&self, symbol: &str, market_id: Option<u64>) -> StreamPlan;
    /// Fill the per-symbol gaps the bulk catalog cannot provide
    /// (true BBO for HL/Entropy, open interest for AsterDEX).
    /// Default: no-op — a venue whose bulk rows are already
    /// complete does nothing.
    async fn enrich(&self, http: &reqwest::Client, row: &mut MarketInfo) -> Result<(), String> {
        let _ = (http, row);
        Ok(())
    }
}

/// The registry: adding a venue is one directory + one line here.
/// Hyperliquid / Entropy moved in PR-1.2; the rest follow in 1.3/1.4.
pub fn all() -> &'static [&'static dyn Venue] {
    static ALL: [&'static dyn Venue; 2] = [&hyperliquid::HYPERLIQUID, &hyperliquid::ENTROPY];
    &ALL
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_lists_the_moved_venues() {
        // PR-1.2: the first two venues are registered; the assembly
        // path iterates all() and dispatches to the registered venue
        // first (the real AppState assembly still needs ./data +
        // SQLite + vault, so it stays a manual check).
        let ids: Vec<&str> = all().iter().map(|v| v.id()).collect();
        assert_eq!(ids, ["hyperliquid", "entropy"]);
        let mut visited = 0;
        for venue in all() {
            let _ = venue.credentials();
            visited += 1;
        }
        assert_eq!(visited, 2);
    }
}
