//! Position reconciliation state (phase 5A §3, spec §3).
//!
//! Two rules live here:
//!
//! 1. **Post-fill grace (5s).** After a fill, the exchange needs a
//!    moment to settle. A REST poll landing inside the grace window
//!    would return a pre-fill snapshot and overwrite the fresh local
//!    estimate — so polls skip their write while the grace holds, and
//!    forced reconciles wait it out first.
//! 2. **A failed query is never a zero position.** Polls only ever
//!    overwrite the live snapshot on success; on error the last known
//!    positions stay exactly as they were (the adapters' `poll_account`
//!    already behaves this way — this module only adds the grace).
//!
//! The exchange-confirmed account state is always the source of truth
//! in the end: the local estimate (see `SyncManager::apply_fill`) is
//! only a real-time approximation until the next post-grace poll lands.

use std::collections::HashMap;
use std::sync::Mutex;

use crate::core::util::now_ms;

/// Post-fill reconciliation grace, in milliseconds.
pub const RECONCILE_GRACE_MS: i64 = 5_000;

pub struct ReconcileState {
    last_fill_at: Mutex<HashMap<String, i64>>,
}

impl ReconcileState {
    pub fn new() -> Self {
        Self {
            last_fill_at: Mutex::new(HashMap::new()),
        }
    }

    /// Record a fill for an account: starts its grace window.
    pub fn note_fill(&self, account_id: &str) {
        self.note_fill_at(account_id, now_ms());
    }

    pub fn note_fill_at(&self, account_id: &str, now: i64) {
        if let Ok(mut m) = self.last_fill_at.lock() {
            m.insert(account_id.to_string(), now);
        }
    }

    /// True while the account is inside its post-fill grace window.
    pub fn in_grace(&self, account_id: &str) -> bool {
        self.in_grace_at(account_id, now_ms())
    }

    pub fn in_grace_at(&self, account_id: &str, now: i64) -> bool {
        self.grace_remaining_ms_at(account_id, now) > 0
    }

    /// Milliseconds until the grace window ends (0 when not in grace).
    pub fn grace_remaining_ms(&self, account_id: &str) -> i64 {
        self.grace_remaining_ms_at(account_id, now_ms())
    }

    pub fn grace_remaining_ms_at(&self, account_id: &str, now: i64) -> i64 {
        let last = self
            .last_fill_at
            .lock()
            .map(|m| m.get(account_id).copied())
            .unwrap_or(None);
        match last {
            Some(t) => (t + RECONCILE_GRACE_MS - now).max(0),
            None => 0,
        }
    }
}

impl Default for ReconcileState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grace_window_opens_and_closes() {
        let s = ReconcileState::new();
        assert!(!s.in_grace_at("a", 1000));
        assert_eq!(s.grace_remaining_ms_at("a", 1000), 0);
        s.note_fill_at("a", 1000);
        assert!(s.in_grace_at("a", 1000));
        assert!(s.in_grace_at("a", 5999));
        assert_eq!(s.grace_remaining_ms_at("a", 3000), 3000);
        assert!(!s.in_grace_at("a", 6000));
        assert_eq!(s.grace_remaining_ms_at("a", 6000), 0);
        // Another fill re-opens the window.
        s.note_fill_at("a", 7000);
        assert!(s.in_grace_at("a", 11000));
        // Other accounts are unaffected.
        assert!(!s.in_grace_at("b", 8000));
    }
}
