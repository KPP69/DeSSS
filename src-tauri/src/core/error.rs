//! One error type for every venue (v3 §2.1), with a kind.
//! Callers decide "was it sent?" with [`VenueError::pre_send`],
//! never by matching error text.

pub type VenueResult<T> = Result<T, VenueError>;

/// One error type for every venue, with a kind (v3 §2.1). Callers
/// decide "was it sent?" with [`VenueError::pre_send`], never by
/// matching error text.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum VenueError {
    /// No live connection to the exchange.
    #[error("未连接")]
    NotConnected,
    /// The exchange explicitly refused the request before acting on
    /// it (bad params, insufficient margin, unknown order for a
    /// cancel): definitely NOT sent.
    #[error("交易所拒绝：{msg}")]
    Rejected {
        /// Exchange error code, when the exchange gave one.
        code: Option<i64>,
        msg: String,
    },
    /// The request was sent (or may have been sent) but no usable
    /// answer came back — timeout, transport failure, unreadable
    /// response. The outcome on the exchange is UNKNOWN: the caller
    /// must reconcile by querying the order, never guess filled or
    /// failed. Only write paths produce this; reads produce
    /// `Transport` / `Rejected` instead.
    #[error("结果未知，需对账：{0}")]
    UnknownOutcome(String),
    /// The venue does not implement this capability yet.
    #[error("该交易所暂不支持：{0}")]
    Unsupported(&'static str),
    /// Transport failure talking to the exchange (connect, DNS, TLS,
    /// non-2xx, unreadable body). The outcome is unknown.
    #[error("传输失败：{0}")]
    Transport(String),
}

impl VenueError {
    /// Exchange refusal without an exchange error code — the common
    /// case (local validation, missing credentials, unknown order).
    /// Carries no code; use the struct literal only when the
    /// exchange actually gave one.
    pub fn rejected(msg: impl Into<String>) -> Self {
        Self::Rejected {
            code: None,
            msg: msg.into(),
        }
    }

    /// Transport failure (network, timeout, unreadable response).
    pub fn transport(msg: impl Into<String>) -> Self {
        Self::Transport(msg.into())
    }

    /// Capability the venue does not implement yet.
    pub fn unsupported(op: &'static str) -> Self {
        Self::Unsupported(op)
    }

    /// True only when the error proves the request was NOT sent (or
    /// was rejected before acting): the caller must NOT reconcile.
    /// Every other variant means the outcome is unknown — the caller
    /// must query the exchange.
    pub fn pre_send(&self) -> bool {
        matches!(
            self,
            VenueError::NotConnected | VenueError::Rejected { .. } | VenueError::Unsupported(_)
        )
    }
}

/// The exchange's exact "no such order" signal (orderStatus
/// `"status": "unknownOid"`). The single source of truth for the
/// "this order does not exist" decision: producers return it,
/// consumers match on it, and nothing else may invent the string.
pub const UNKNOWN_OID: &str = "unknownOid";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pre_send_only_for_definite_not_sent() {
        assert!(VenueError::NotConnected.pre_send());
        assert!(VenueError::Rejected {
            code: Some(1001),
            msg: "bad params".to_string()
        }
        .pre_send());
        assert!(VenueError::unsupported("order").pre_send());
        assert!(!VenueError::UnknownOutcome("timeout".to_string()).pre_send());
        assert!(!VenueError::transport("reset").pre_send());
    }
}
