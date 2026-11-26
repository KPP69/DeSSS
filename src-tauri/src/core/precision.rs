//! Strict order precision with `rust_decimal` (phase 5A §3).
//!
//! Prices and quantities are aligned in `Decimal`, never in `f64`:
//! float error must never fudge an order onto a wrong tick. The tick /
//! step need not be a power of ten — `0.5` and `5` work exactly the same
//! as `0.01`, because alignment is integer division on the step, not
//! decimal-place counting.
//!
//! Direction matters for limit orders: a buy aligned UP would pay more
//! than the user asked, a sell aligned DOWN would accept less. The order
//! path aligns buys down / sells up; the risk gateway then verifies the
//! result is exactly aligned and rejects anything that is not.

use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;

/// `f64` → `Decimal` via its shortest string form. Order inputs arrive
/// as JSON numbers, so this is the honest conversion — no binary float
/// residue leaks into the alignment math.
pub fn to_decimal(v: f64) -> Decimal {
    Decimal::from_f64(v).unwrap_or(Decimal::ZERO)
}

/// Round `value` DOWN to a whole multiple of `step`.
pub fn align_down(value: Decimal, step: Decimal) -> Decimal {
    if step <= Decimal::ZERO {
        return value;
    }
    (value / step).floor() * step
}

/// Round `value` UP to a whole multiple of `step`.
pub fn align_up(value: Decimal, step: Decimal) -> Decimal {
    if step <= Decimal::ZERO {
        return value;
    }
    (value / step).ceil() * step
}

/// Round `value` to the NEAREST multiple of `step` (ties away from zero).
pub fn align_nearest(value: Decimal, step: Decimal) -> Decimal {
    if step <= Decimal::ZERO {
        return value;
    }
    (value / step).round() * step
}

/// True when `value` is exactly a whole multiple of `step`.
pub fn is_aligned(value: Decimal, step: Decimal) -> bool {
    if step <= Decimal::ZERO {
        return true;
    }
    value % step == Decimal::ZERO
}

/// Align a limit price for a side: buys round down (never pay more than
/// asked), sells round up (never accept less than asked).
pub fn align_price_for_side(price: Decimal, tick: Decimal, is_buy: bool) -> Decimal {
    if is_buy {
        align_down(price, tick)
    } else {
        align_up(price, tick)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::dec;

    #[test]
    fn tick_half_rounds_both_ways() {
        let tick = dec!(0.5);
        assert_eq!(align_down(dec!(100.3), tick), dec!(100.0));
        assert_eq!(align_up(dec!(100.3), tick), dec!(100.5));
        assert_eq!(align_nearest(dec!(100.3), tick), dec!(100.5));
        assert_eq!(align_nearest(dec!(100.2), tick), dec!(100.0));
        assert!(is_aligned(dec!(100.5), tick));
        assert!(!is_aligned(dec!(100.3), tick));
    }

    #[test]
    fn tick_five_rounds_both_ways() {
        let tick = dec!(5);
        assert_eq!(align_down(dec!(103), tick), dec!(100));
        assert_eq!(align_up(dec!(103), tick), dec!(105));
        assert_eq!(align_nearest(dec!(102), tick), dec!(100));
        assert_eq!(align_nearest(dec!(103), tick), dec!(105));
        assert!(is_aligned(dec!(105), tick));
        assert!(!is_aligned(dec!(103), tick));
    }

    #[test]
    fn tick_cent_and_exact_values() {
        let tick = dec!(0.01);
        assert_eq!(align_down(dec!(90000.005), tick), dec!(90000.00));
        assert_eq!(align_up(dec!(90000.005), tick), dec!(90000.01));
        assert!(is_aligned(dec!(90000.00), tick));
        // An already-aligned value is unchanged by every direction.
        assert_eq!(align_down(dec!(100.5), dec!(0.5)), dec!(100.5));
        assert_eq!(align_up(dec!(100.5), dec!(0.5)), dec!(100.5));
    }

    #[test]
    fn side_aware_price_alignment() {
        let tick = dec!(0.5);
        // Buy 100.3 → 100.0 (never pay more than asked).
        assert_eq!(align_price_for_side(dec!(100.3), tick, true), dec!(100.0));
        // Sell 100.3 → 100.5 (never accept less than asked).
        assert_eq!(align_price_for_side(dec!(100.3), tick, false), dec!(100.5));
    }

    #[test]
    fn degenerate_step_is_a_noop() {
        assert_eq!(align_down(dec!(100.3), Decimal::ZERO), dec!(100.3));
        assert_eq!(align_up(dec!(100.3), Decimal::ZERO), dec!(100.3));
        assert!(is_aligned(dec!(100.3), Decimal::ZERO));
    }

    #[test]
    fn f64_conversion_is_exact_for_order_inputs() {
        // 0.1 as f64 is 0.1000000000000000055…; Decimal takes the
        // shortest repr, so alignment math sees exactly 0.1.
        assert_eq!(to_decimal(0.1), dec!(0.1));
        assert_eq!(to_decimal(90000.5), dec!(90000.5));
    }
}
