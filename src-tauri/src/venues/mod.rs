//! One venue, one directory: the registry every exchange plugs into
//! (v3 §1.1). Adding an exchange is one new directory + one line in
//! [`all()`]; this round registers nothing, so `all()` is empty and
//! the assembly loop over it is a no-op.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;

use crate::adapters::market_catalog::MarketInfo;
use crate::adapters::reconcile::ReconcileState;
use crate::adapters::ExchangeAdapter;
use crate::bus::EventBus;
use crate::core::types::AccountLive;
use crate::data_engine::realtime::StreamKind;

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
}

/// The registry: adding a venue is one directory + one line here.
/// Empty this round — no venue has moved yet (PR-1.2 starts with
/// Hyperliquid / Entropy).
pub fn all() -> &'static [&'static dyn Venue] {
    &[]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_registry_assembles_without_panic() {
        // The assembly path iterates all() and builds one adapter per
        // venue; with no venues registered it must simply do nothing.
        let mut built = 0;
        for venue in all() {
            let _ = venue.id();
            built += 1;
        }
        assert_eq!(built, 0);
        assert!(all().is_empty());
    }
}
