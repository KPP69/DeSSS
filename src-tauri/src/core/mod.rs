//! Exchange-independent foundation (v3 §1): shared DTOs, Decimal
//! precision, the order book and the symbol rule. One thing, one
//! place — everything here is used by every venue and by no venue in
//! particular.

pub mod book;
pub mod error;
pub mod market;
pub mod precision;
pub mod signing;
pub mod stream_kind;
pub mod symbol;
pub mod types;
pub mod util;
