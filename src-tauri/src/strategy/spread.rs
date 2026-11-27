//! Mean-reversion spread evaluation: a pure function of two books,
//! two market specs, the parameters and the armed state. No I/O, no
//! timers — every rule below is covered by unit tests here.
//!
//! The spread is quoted in leg-A units. Two premiums are tracked:
//!
//!   - sell-A premium: `bid_a / ask_b - 1` (A expensive vs B)
//!   - buy-A premium: `bid_b / ask_a - 1` (A cheap vs B)
//!
//! Each must clear its threshold — midline ± the leg's band, plus the
//! two legs' fees — and hold for `persist_sec` before firing.
//!
//! The returned intents are `[leg-A intent, leg-B intent]` in that
//! order; the runner attaches the account / exchange / symbol.

use crate::core::market::MarketInfo;
use crate::core::types::OrderBook;

use super::SpreadParams;

/// In-memory armed state for one instance (restart resets it).
#[derive(Clone, Debug, Default)]
pub struct SpreadState {
    pub armed_at_ms: Option<i64>,
    pub armed_side: Option<String>,
}

impl SpreadState {
    pub fn clear(&mut self) {
        self.armed_at_ms = None;
        self.armed_side = None;
    }
}

/// One side of the book the evaluator reads: levels best-first plus
/// the receipt time of the last frame.
#[derive(Clone, Debug)]
pub struct BookSnapshot {
    pub bids: Vec<(f64, f64)>,
    pub asks: Vec<(f64, f64)>,
    pub last_update_ms: i64,
}

impl BookSnapshot {
    pub fn from_order_book(book: &OrderBook) -> Self {
        Self {
            bids: book.bids.iter().map(|l| (l.0, l.1)).collect(),
            asks: book.asks.iter().map(|l| (l.0, l.1)).collect(),
            last_update_ms: book.ts,
        }
    }

    pub fn best_bid(&self) -> Option<f64> {
        self.bids.first().map(|l| l.0)
    }

    pub fn best_ask(&self) -> Option<f64> {
        self.asks.first().map(|l| l.0)
    }
}

/// Static contract facts the evaluator needs, from the catalog row.
/// `None` when the row has no usable step — without a step the
/// quantity cannot be aligned, so the runner skips the round.
#[derive(Clone, Debug)]
pub struct MarketSpec {
    pub tick_size: f64,
    pub step_size: f64,
    pub min_notional: Option<f64>,
}

impl MarketSpec {
    pub fn from_row(row: &MarketInfo) -> Option<Self> {
        let step = row.step_size.filter(|s| *s > 0.0)?;
        Some(Self {
            tick_size: row.tick_size.unwrap_or(0.0),
            step_size: step,
            min_notional: row.min_notional,
        })
    }
}

/// One leg's order intent, in leg order `[A, B]`. The runner fills
/// in the account / exchange / symbol and the `strategy:<id>` origin.
#[derive(Clone, Debug, PartialEq)]
pub struct PlaceIntent {
    /// "buy" | "sell".
    pub side: String,
    pub order_type: String,
    pub quantity: f64,
    pub price: f64,
}

/// A book older than this is stale: the evaluator returns None and
/// clears the armed state (spec 4a).
pub const STALE_MS: i64 = 5_000;

/// Protection buffer, in bps, applied both to the depth walk (θ) and
/// to the limit prices (1 ∓ protection). The order does not name a
/// value; 2 bps is the chosen default — small enough not to eat the
/// edge, large enough to survive a one-tick adverse move between the
/// signal and the fill. Tune here, in one place.
pub const PROTECTION_BPS: f64 = 2.0;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Dir {
    SellA,
    BuyA,
}

/// Both premiums in bps: (sell-A, buy-A). `None` on a side whose
/// top-of-book is missing.
pub fn premiums(a: &BookSnapshot, b: &BookSnapshot) -> (Option<f64>, Option<f64>) {
    let sell_a = match (a.best_bid(), b.best_ask()) {
        (Some(bid_a), Some(ask_b)) if ask_b > 0.0 => Some((bid_a / ask_b - 1.0) * 10_000.0),
        _ => None,
    };
    let buy_a = match (b.best_bid(), a.best_ask()) {
        (Some(bid_b), Some(ask_a)) if ask_a > 0.0 => Some((bid_b / ask_a - 1.0) * 10_000.0),
        _ => None,
    };
    (sell_a, buy_a)
}

/// Quantity executable at the required premium: walk both books level
/// by level, accumulating `min(remaining)` while the post-fee prices
/// still clear the threshold by θ. `sell_a` selects the sides
/// (sell A at its bids / buy B at its asks, or the mirror).
fn executable_qty(
    a: &BookSnapshot,
    b: &BookSnapshot,
    fee_a: f64,
    fee_b: f64,
    theta: f64,
    sell_a: bool,
) -> f64 {
    let (levels_a, levels_b) = if sell_a {
        (&a.bids, &b.asks)
    } else {
        (&a.asks, &b.bids)
    };
    let mut i = 0usize;
    let mut j = 0usize;
    let mut rem_a = 0.0f64;
    let mut rem_b = 0.0f64;
    let mut accum = 0.0f64;
    loop {
        if rem_a <= 0.0 {
            let Some(l) = levels_a.get(i) else { break };
            rem_a = l.1;
            i += 1;
        }
        if rem_b <= 0.0 {
            let Some(l) = levels_b.get(j) else { break };
            rem_b = l.1;
            j += 1;
        }
        // Prices of the levels being consumed: A-side level i-1,
        // B-side level j-1.
        let (px_a, px_b) = (levels_a[i - 1].0, levels_b[j - 1].0);
        // The executable price on the A side, net of its fee, must
        // still beat the B side's price, grossed up by its fee and
        // the protection θ — otherwise this depth is not really
        // there at the required premium.
        let (net_a, gross_b) = if sell_a {
            (px_a * (1.0 - fee_a), px_b * (1.0 + fee_b) * (1.0 + theta))
        } else {
            (px_b * (1.0 - fee_b), px_a * (1.0 + fee_a) * (1.0 + theta))
        };
        if net_a < gross_b {
            break;
        }
        let take = rem_a.min(rem_b);
        accum += take;
        rem_a -= take;
        rem_b -= take;
    }
    accum
}

/// Align a quantity down to a multiple of both steps. Steps for one
/// asset across venues are equal or decimal multiples, so aligning to
/// the coarser step is already a multiple of the finer one; the loop
/// steps down by the coarse step until it is, and gives up (0.0)
/// when no such multiple exists.
fn align_qty(qty: f64, step_a: f64, step_b: f64) -> f64 {
    if qty <= 0.0 || step_a <= 0.0 || step_b <= 0.0 {
        return 0.0;
    }
    let coarse = step_a.max(step_b);
    let fine = step_a.min(step_b);
    let mut aligned = (qty / coarse).floor() * coarse;
    for _ in 0..1000 {
        if aligned <= 0.0 {
            return 0.0;
        }
        let rem = aligned / fine;
        if (rem - rem.round()).abs() < 1e-9 * rem.abs().max(1.0) {
            return aligned;
        }
        aligned -= coarse;
    }
    0.0
}

/// Evaluate one spread instance. Returns the two leg intents
/// `[A, B]` when a signal fires, else `None`.
///
/// - (a) Either book stale → None, armed cleared.
/// - (b) Premium vs threshold (midline ± band + both fees).
/// - (c) The direction must persist `persist_sec` (armed state).
/// - (d) Position state machine: flat → any open; short A → only
///   buy-A (close); long A → only sell-A (close).
/// - (e) Quantity from the executable depth, capped by
///   `take_fraction` and `max_notional_usd`, step-aligned, and
///   above both legs' minimum notional.
/// - (f) Limit = the opposite side's top price × (1 ∓ protection).
// Eight parameters is the spec-fixed signature (two books, two
// specs, params, armed state, net qty, now) — not a smell to
// refactor away.
#[allow(clippy::too_many_arguments)]
pub fn evaluate(
    a: &BookSnapshot,
    b: &BookSnapshot,
    spec_a: &MarketSpec,
    spec_b: &MarketSpec,
    p: &SpreadParams,
    st: &mut SpreadState,
    net_qty: f64,
    now_ms: i64,
) -> Option<Vec<PlaceIntent>> {
    if now_ms - a.last_update_ms > STALE_MS || now_ms - b.last_update_ms > STALE_MS {
        st.clear();
        return None;
    }
    let fees_bps = p.fee_bps_a + p.fee_bps_b;
    let (prem_sell, prem_buy) = premiums(a, b);
    let dir = prem_sell
        .filter(|prem| *prem > p.midline_bps + p.upper_bps + fees_bps)
        .map(|_| Dir::SellA)
        .or_else(|| {
            prem_buy
                .filter(|prem| *prem > p.lower_bps - p.midline_bps + fees_bps)
                .map(|_| Dir::BuyA)
        });
    let Some(dir) = dir else {
        st.clear();
        return None;
    };
    // (d) position state machine: net_qty is the signed A position
    // (+ long, − short); a position smaller than one step is flat.
    let allowed = if net_qty.abs() < spec_a.step_size {
        true
    } else if net_qty < 0.0 {
        dir == Dir::BuyA
    } else {
        dir == Dir::SellA
    };
    if !allowed {
        st.clear();
        return None;
    }
    // (c) persistence gate: a new direction re-arms from now.
    let side = match dir {
        Dir::SellA => "sell_a",
        Dir::BuyA => "buy_a",
    };
    if st.armed_side.as_deref() != Some(side) {
        st.armed_side = Some(side.to_string());
        st.armed_at_ms = Some(now_ms);
    }
    let armed_at = st.armed_at_ms.unwrap_or(now_ms);
    if now_ms - armed_at < (p.persist_sec * 1000.0) as i64 {
        return None;
    }
    st.clear();
    // (e) quantity: executable depth, capped, step-aligned.
    let theta = PROTECTION_BPS / 10_000.0;
    let fee_a = p.fee_bps_a / 10_000.0;
    let fee_b = p.fee_bps_b / 10_000.0;
    let accum = executable_qty(a, b, fee_a, fee_b, theta, dir == Dir::SellA);
    if accum <= 0.0 {
        return None;
    }
    let (buy_price, sell_price) = match dir {
        Dir::SellA => (b.best_ask().unwrap(), a.best_bid().unwrap()),
        Dir::BuyA => (a.best_ask().unwrap(), b.best_bid().unwrap()),
    };
    let qty = (accum * p.take_fraction).min(p.max_notional_usd / buy_price);
    let qty = align_qty(qty, spec_a.step_size, spec_b.step_size);
    if qty <= 0.0 {
        return None;
    }
    if let Some(min) = spec_a.min_notional {
        let notional_a = qty
            * if dir == Dir::SellA {
                sell_price
            } else {
                buy_price
            };
        if notional_a < min {
            return None;
        }
    }
    if let Some(min) = spec_b.min_notional {
        let notional_b = qty
            * if dir == Dir::SellA {
                buy_price
            } else {
                sell_price
            };
        if notional_b < min {
            return None;
        }
    }
    // (f) limits: sell at the bid less protection, buy at the ask
    // plus protection.
    let prot = PROTECTION_BPS / 10_000.0;
    let (intent_a, intent_b) = match dir {
        Dir::SellA => (
            PlaceIntent {
                side: "sell".to_string(),
                order_type: "limit".to_string(),
                quantity: qty,
                price: a.best_bid().unwrap() * (1.0 - prot),
            },
            PlaceIntent {
                side: "buy".to_string(),
                order_type: "limit".to_string(),
                quantity: qty,
                price: b.best_ask().unwrap() * (1.0 + prot),
            },
        ),
        Dir::BuyA => (
            PlaceIntent {
                side: "buy".to_string(),
                order_type: "limit".to_string(),
                quantity: qty,
                price: a.best_ask().unwrap() * (1.0 + prot),
            },
            PlaceIntent {
                side: "sell".to_string(),
                order_type: "limit".to_string(),
                quantity: qty,
                price: b.best_bid().unwrap() * (1.0 - prot),
            },
        ),
    };
    Some(vec![intent_a, intent_b])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::strategy::testkit;

    fn book(bids: &[(f64, f64)], asks: &[(f64, f64)], ts: i64) -> BookSnapshot {
        BookSnapshot {
            bids: bids.to_vec(),
            asks: asks.to_vec(),
            last_update_ms: ts,
        }
    }

    fn spec(step: f64, min_notional: Option<f64>) -> MarketSpec {
        MarketSpec {
            tick_size: 0.5,
            step_size: step,
            min_notional,
        }
    }

    /// A wide spread: A bid 91000 / B ask 90000 → sell-A premium
    /// 111.1 bps, buy-A premium deeply negative.
    fn wide_books(ts: i64) -> (BookSnapshot, BookSnapshot) {
        (
            book(&[(91000.0, 1.0)], &[(91010.0, 1.0)], ts),
            book(&[(89990.0, 1.0)], &[(90000.0, 1.0)], ts),
        )
    }

    fn params() -> SpreadParams {
        testkit::params()
    }

    #[test]
    fn fires_above_threshold_flat_book() {
        let (a, b) = wide_books(1000);
        let mut st = SpreadState::default();
        let intents = evaluate(
            &a,
            &b,
            &spec(0.001, Some(10.0)),
            &spec(0.001, Some(10.0)),
            &params(),
            &mut st,
            0.0,
            1000,
        )
        .expect("signal fires");
        assert_eq!(intents.len(), 2);
        // A sells at its bid less 2 bps protection; B buys at its ask
        // plus 2 bps. Same quantity on both legs.
        assert_eq!(intents[0].side, "sell");
        assert_eq!(intents[1].side, "buy");
        assert!((intents[0].price - 91000.0 * 0.9998).abs() < 1e-9);
        assert!((intents[1].price - 90000.0 * 1.0002).abs() < 1e-9);
        assert_eq!(intents[0].quantity, intents[1].quantity);
        // Depth 1.0, take 1.0, notional cap 5000/90000 = 0.0555…,
        // step-aligned to 0.055.
        assert!((intents[0].quantity - 0.055).abs() < 1e-12);
        // Firing clears the armed state.
        assert!(st.armed_side.is_none());
    }

    #[test]
    fn inside_threshold_does_not_fire() {
        let (a, b) = wide_books(1000);
        let mut p = params();
        // Threshold 111.2 bps > the 111.1 bps premium.
        p.upper_bps = 111.2;
        let mut st = SpreadState::default();
        assert!(evaluate(
            &a,
            &b,
            &spec(0.001, None),
            &spec(0.001, None),
            &p,
            &mut st,
            0.0,
            1000
        )
        .is_none());
        // Exactly at the threshold does not fire either.
        let prem = (91000.0f64 / 90000.0 - 1.0) * 10_000.0;
        p.upper_bps = prem;
        assert!(evaluate(
            &a,
            &b,
            &spec(0.001, None),
            &spec(0.001, None),
            &p,
            &mut st,
            0.0,
            1000
        )
        .is_none());
    }

    #[test]
    fn persistence_gate_holds_until_persist_sec() {
        let (a, b) = wide_books(1000);
        let mut p = params();
        p.persist_sec = 5.0;
        let mut st = SpreadState::default();
        // First sight arms but does not fire.
        assert!(evaluate(
            &a,
            &b,
            &spec(0.001, None),
            &spec(0.001, None),
            &p,
            &mut st,
            0.0,
            1000
        )
        .is_none());
        assert_eq!(st.armed_side.as_deref(), Some("sell_a"));
        // 4.9s later: still held.
        assert!(evaluate(
            &a,
            &b,
            &spec(0.001, None),
            &spec(0.001, None),
            &p,
            &mut st,
            0.0,
            5900
        )
        .is_none());
        // 5s: fires.
        assert!(evaluate(
            &a,
            &b,
            &spec(0.001, None),
            &spec(0.001, None),
            &p,
            &mut st,
            0.0,
            6000
        )
        .is_some());
    }

    #[test]
    fn direction_change_rearms() {
        // Sell-A premium first, then the mirror: A cheap vs B.
        let (a, b) = wide_books(1000);
        let mut p = params();
        p.persist_sec = 5.0;
        let mut st = SpreadState::default();
        assert!(evaluate(
            &a,
            &b,
            &spec(0.001, None),
            &spec(0.001, None),
            &p,
            &mut st,
            0.0,
            1000
        )
        .is_none());
        assert_eq!(st.armed_side.as_deref(), Some("sell_a"));
        // Mirror books: A ask 90000 / B bid 91000 → buy-A premium 111 bps.
        let a2 = book(&[(89990.0, 1.0)], &[(90000.0, 1.0)], 2000);
        let b2 = book(&[(91000.0, 1.0)], &[(91010.0, 1.0)], 2000);
        assert!(evaluate(
            &a2,
            &b2,
            &spec(0.001, None),
            &spec(0.001, None),
            &p,
            &mut st,
            0.0,
            2000
        )
        .is_none());
        assert_eq!(st.armed_side.as_deref(), Some("buy_a"));
        assert_eq!(st.armed_at_ms, Some(2000));
    }

    #[test]
    fn stale_book_returns_none_and_clears_armed() {
        let (a, b) = wide_books(1000);
        let mut p = params();
        p.persist_sec = 5.0;
        let mut st = SpreadState::default();
        assert!(evaluate(
            &a,
            &b,
            &spec(0.001, None),
            &spec(0.001, None),
            &p,
            &mut st,
            0.0,
            1000
        )
        .is_none());
        assert!(st.armed_side.is_some());
        // B's book is 5.001s old: stale.
        let b_old = book(&[(89990.0, 1.0)], &[(90000.0, 1.0)], 1000);
        assert!(evaluate(
            &a,
            &b_old,
            &spec(0.001, None),
            &spec(0.001, None),
            &p,
            &mut st,
            0.0,
            6001
        )
        .is_none());
        assert!(st.armed_side.is_none());
    }

    #[test]
    fn depth_walk_takes_only_executable_levels() {
        // A bids: 91000 x 0.02 (clears), then 90500 x 1.0 (post-fee
        // 90500 < 90000*1.0002 = 90018? No: 90500 > 90018, clears
        // too — use 89900 for the second level so it fails).
        let a = book(&[(91000.0, 0.02), (89900.0, 1.0)], &[(91010.0, 1.0)], 1000);
        let b = book(&[(89990.0, 1.0)], &[(90000.0, 0.5)], 1000);
        let mut p = params();
        p.take_fraction = 1.0;
        p.max_notional_usd = 100_000.0;
        let mut st = SpreadState::default();
        let intents = evaluate(
            &a,
            &b,
            &spec(0.001, None),
            &spec(0.001, None),
            &p,
            &mut st,
            0.0,
            1000,
        )
        .expect("signal fires");
        // Only the first level pair clears: min(0.02, 0.5) = 0.02.
        assert!((intents[0].quantity - 0.02).abs() < 1e-12);
    }

    #[test]
    fn aligned_to_zero_or_below_min_notional_returns_none() {
        let (a, b) = wide_books(1000);
        // Step 1.0: the 0.055 qty aligns to 0.
        let mut st = SpreadState::default();
        assert!(evaluate(
            &a,
            &b,
            &spec(1.0, None),
            &spec(1.0, None),
            &params(),
            &mut st,
            0.0,
            1000
        )
        .is_none());
        // Notional 0.055 * 90000 ≈ 4950 < 5000 min on leg B.
        let mut st = SpreadState::default();
        assert!(evaluate(
            &a,
            &b,
            &spec(0.001, Some(10.0)),
            &spec(0.001, Some(5000.0)),
            &params(),
            &mut st,
            0.0,
            1000
        )
        .is_none());
    }

    #[test]
    fn position_state_machine() {
        let (a, b) = wide_books(1000);
        let mut st = SpreadState::default();
        // Flat: sell-A signal fires.
        assert!(evaluate(
            &a,
            &b,
            &spec(0.001, None),
            &spec(0.001, None),
            &params(),
            &mut st,
            0.0,
            1000
        )
        .is_some());
        // Short A (-0.1): only the buy-A direction may fire; the
        // sell-A signal is ignored.
        let mut st = SpreadState::default();
        assert!(evaluate(
            &a,
            &b,
            &spec(0.001, None),
            &spec(0.001, None),
            &params(),
            &mut st,
            -0.1,
            1000
        )
        .is_none());
        // Long A (+0.1): the sell-A signal fires (close direction).
        let mut st = SpreadState::default();
        assert!(evaluate(
            &a,
            &b,
            &spec(0.001, None),
            &spec(0.001, None),
            &params(),
            &mut st,
            0.1,
            1000
        )
        .is_some());
        // A dust position below one step counts as flat.
        let mut st = SpreadState::default();
        assert!(evaluate(
            &a,
            &b,
            &spec(0.001, None),
            &spec(0.001, None),
            &params(),
            &mut st,
            0.0005,
            1000
        )
        .is_some());
        // Mirror: short A + buy-A signal fires.
        let a2 = book(&[(89990.0, 1.0)], &[(90000.0, 1.0)], 1000);
        let b2 = book(&[(91000.0, 1.0)], &[(91010.0, 1.0)], 1000);
        let mut st = SpreadState::default();
        assert!(evaluate(
            &a2,
            &b2,
            &spec(0.001, None),
            &spec(0.001, None),
            &params(),
            &mut st,
            -0.1,
            1000
        )
        .is_some());
        // Long A + buy-A signal is ignored (would add).
        let mut st = SpreadState::default();
        assert!(evaluate(
            &a2,
            &b2,
            &spec(0.001, None),
            &spec(0.001, None),
            &params(),
            &mut st,
            0.1,
            1000
        )
        .is_none());
    }

    #[test]
    fn fees_raise_the_threshold() {
        let (a, b) = wide_books(1000);
        let mut p = params();
        // 111.1 bps premium vs threshold 10 + 200 fees = 210: no fire.
        p.fee_bps_a = 100.0;
        p.fee_bps_b = 100.0;
        let mut st = SpreadState::default();
        assert!(evaluate(
            &a,
            &b,
            &spec(0.001, None),
            &spec(0.001, None),
            &p,
            &mut st,
            0.0,
            1000
        )
        .is_none());
    }

    #[test]
    fn align_qty_needs_a_multiple_of_both_steps() {
        assert!((align_qty(0.0555, 0.001, 0.001) - 0.055).abs() < 1e-12);
        // The fine step does not loosen the coarse one: 0.0555 is a
        // multiple of 0.0001 but not of 0.001, so the result is 0.055.
        assert!((align_qty(0.0555, 0.001, 0.0001) - 0.055).abs() < 1e-12);
        assert_eq!(align_qty(0.0005, 0.001, 0.001), 0.0);
        // 0.002 / 0.005 steps: 0.015 is not a multiple of 0.002,
        // the walk steps down to 0.01.
        assert!((align_qty(0.015, 0.002, 0.005) - 0.01).abs() < 1e-12);
    }
}
