//! Pure in-memory order-book state machine shared by the real adapters.
//!
//! No I/O, no timers — every transition is a pure function, so the
//! continuity rules are covered by unit tests here instead of against a
//! live exchange.
//!
//! - Hyperliquid `l2Book` frames carry the full book: use `apply_snapshot`.
//! - Lighter `order_book` frames are incremental: use `apply_delta`, which
//!   validates continuity with `begin_nonce` only. Lighter's `offset` is
//!   explicitly NOT a continuity counter (the docs say it is server-scoped
//!   and may jump), so it is never used as a gap signal.

use crate::adapters::types::{OrderBook, PriceLevel};

/// One side of the book, kept sorted best-first after every mutation.
#[derive(Clone, Debug, Default)]
struct Side {
    /// (price, qty), sorted best-first (bids desc, asks asc).
    levels: Vec<(f64, f64)>,
    desc: bool,
}

impl Side {
    fn new(desc: bool) -> Self {
        Self {
            levels: Vec::new(),
            desc,
        }
    }

    fn replace(&mut self, levels: &[(f64, f64)]) {
        self.levels = levels.to_vec();
        self.sort();
    }

    fn apply(&mut self, levels: &[(f64, f64)]) {
        for &(px, qty) in levels {
            if let Some(pos) = self.levels.iter().position(|(p, _)| *p == px) {
                if qty == 0.0 {
                    self.levels.remove(pos);
                } else {
                    self.levels[pos].1 = qty;
                }
            } else if qty != 0.0 {
                self.levels.push((px, qty));
            }
        }
        self.sort();
    }

    fn sort(&mut self) {
        if self.desc {
            self.levels
                .sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
        } else {
            self.levels
                .sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        }
    }

    fn top(&self, depth: usize) -> Vec<PriceLevel> {
        self.levels
            .iter()
            .take(depth)
            .map(|&(p, q)| PriceLevel(p, q))
            .collect()
    }
}

/// Local order book for one market.
#[derive(Clone, Debug)]
pub struct BookState {
    bids: Side,
    asks: Side,
    /// Nonce of the last accepted frame (Lighter continuity). None until
    /// the first snapshot arrives.
    last_nonce: Option<u64>,
}

impl BookState {
    pub fn new() -> Self {
        Self {
            bids: Side::new(true),
            asks: Side::new(false),
            last_nonce: None,
        }
    }

    pub fn clear(&mut self) {
        self.bids.levels.clear();
        self.asks.levels.clear();
        self.last_nonce = None;
    }

    pub fn is_empty(&self) -> bool {
        self.bids.levels.is_empty() && self.asks.levels.is_empty()
    }

    /// Full-book frame (Hyperliquid l2Book): replace both sides.
    pub fn apply_snapshot(&mut self, bids: &[(f64, f64)], asks: &[(f64, f64)]) {
        self.bids.replace(bids);
        self.asks.replace(asks);
        self.last_nonce = None;
    }

    /// Incremental frame (Lighter order_book). Continuity is checked with
    /// `begin_nonce` only: it must equal the previous frame's `nonce`.
    /// A mismatch means the stream has a gap and the caller must clear +
    /// resubscribe for a fresh snapshot.
    pub fn apply_delta(
        &mut self,
        bids: &[(f64, f64)],
        asks: &[(f64, f64)],
        begin_nonce: Option<u64>,
        nonce: Option<u64>,
    ) -> Result<(), BookGap> {
        if let (Some(begin), Some(prev)) = (begin_nonce, self.last_nonce) {
            if begin != prev {
                return Err(BookGap {
                    expected: prev,
                    got: begin,
                });
            }
        }
        self.bids.apply(bids);
        self.asks.apply(asks);
        if nonce.is_some() {
            self.last_nonce = nonce;
        }
        Ok(())
    }

    /// Snapshot frame with a nonce (Lighter resync): replace both sides
    /// and record the nonce as the new baseline.
    pub fn apply_snapshot_with_nonce(
        &mut self,
        nonce: Option<u64>,
        bids: &[(f64, f64)],
        asks: &[(f64, f64)],
    ) {
        self.bids.replace(bids);
        self.asks.replace(asks);
        self.last_nonce = nonce;
    }

    pub fn to_book(&self, adapter_id: &str, symbol: &str, depth: usize, ts: i64) -> OrderBook {
        OrderBook {
            adapter_id: adapter_id.to_string(),
            symbol: symbol.to_string(),
            bids: self.bids.top(depth),
            asks: self.asks.top(depth),
            ts,
        }
    }
}

/// Incremental `begin_nonce` does not match the previous frame's nonce:
/// the local book is stale.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BookGap {
    pub expected: u64,
    pub got: u64,
}

impl std::fmt::Display for BookGap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "order book nonce gap: expected begin_nonce == {}, got {}",
            self.expected, self.got
        )
    }
}

impl std::error::Error for BookGap {}

#[cfg(test)]
mod tests {
    use super::*;

    fn state() -> BookState {
        BookState::new()
    }

    #[test]
    fn snapshot_sorts_best_first() {
        let mut b = state();
        b.apply_snapshot(&[(100.0, 1.0), (99.5, 2.0)], &[(101.0, 1.0), (100.5, 3.0)]);
        let book = b.to_book("hl", "BTC", 10, 0);
        assert_eq!(book.bids[0].0, 100.0);
        assert_eq!(book.asks[0].0, 100.5);
    }

    #[test]
    fn delta_upserts_and_removes_zero_qty() {
        let mut b = state();
        b.apply_snapshot_with_nonce(Some(10), &[(100.0, 1.0)], &[(101.0, 1.0)]);
        b.apply_delta(
            &[(100.0, 0.0), (99.0, 5.0)],
            &[(101.0, 2.0)],
            Some(10),
            Some(12),
        )
        .expect("delta");
        let book = b.to_book("li", "BTC-USDC", 10, 0);
        assert_eq!(book.bids.len(), 1);
        assert_eq!(book.bids[0].0, 99.0);
        assert_eq!(book.asks[0].1, 2.0);
    }

    #[test]
    fn delta_rejects_nonce_gap() {
        let mut b = state();
        b.apply_snapshot_with_nonce(Some(10), &[], &[]);
        assert!(b.apply_delta(&[], &[], Some(9), Some(11)).is_err());
        assert!(b.apply_delta(&[], &[], Some(10), Some(11)).is_ok());
        // After the accepted frame the baseline is its nonce (11).
        assert!(b.apply_delta(&[], &[], Some(10), Some(12)).is_err());
        assert!(b.apply_delta(&[], &[], Some(11), Some(12)).is_ok());
    }

    #[test]
    fn clear_resets_nonce_baseline() {
        let mut b = state();
        b.apply_snapshot_with_nonce(Some(10), &[(100.0, 1.0)], &[]);
        b.clear();
        assert!(b.is_empty());
        assert!(b
            .apply_delta(&[(100.0, 1.0)], &[], Some(5), Some(6))
            .is_ok());
    }
}
