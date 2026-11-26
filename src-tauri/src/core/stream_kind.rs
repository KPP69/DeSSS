//! Which exchange protocol a stream speaks. Moved verbatim from
//! `data_engine/realtime.rs` (PR-1.2): the venue's `StreamPlan`
//! names the protocol, and the engine's frame parser dispatches on
//! it — so the enum has to live where both sides can see it.

/// Which exchange protocol a stream speaks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamKind {
    Hyperliquid,
    Lighter,
    Asterdex,
    Arcus,
}
