//! Transitional re-export (PR-1.1): the order book lives in
//! `crate::core::book` now. This module stays until PR-1.4 removes
//! the old paths.

pub use crate::core::book::*;
