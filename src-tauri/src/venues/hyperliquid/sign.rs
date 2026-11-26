//! Hyperliquid / Entropy signing: the agent key and the one-shot
//! `/exchange` write, plus the cancel action builders (pinned to the
//! official SDK's vectors). Moved verbatim from
//! `adapters/hyperliquid.rs` (PR-1.2).

use serde_json::Value;

use super::{HyperliquidAdapter, HL_EXCHANGE_URL, WRITE_TIMEOUT};
use crate::adapters::hl_signing::{self, Mp};
use crate::adapters::AdapterError;

impl HyperliquidAdapter {
    /// The agent key, or a clear error when the account never saved
    /// one (read-only account).
    pub(crate) fn signing_key(&self) -> Result<([u8; 32], String), AdapterError> {
        match (&self.agent_secret, &self.agent_address) {
            (Some(secret), Some(addr)) => Ok((*secret, addr.clone())),
            _ => Err(AdapterError::Other(
                "该账户未保存 Agent 钱包私钥，无法下单".to_string(),
            )),
        }
    }

    /// One `/exchange` write. One-shot, no retry, 30s timeout.
    /// A transport failure, a non-2xx status, or an unreadable body
    /// is `UnknownOutcome` — the order may have landed, so the
    /// caller must reconcile by cloid.
    pub(crate) async fn post_exchange(
        &self,
        action_json: Value,
        action_mp: &Mp<'_>,
        nonce: u64,
    ) -> Result<Value, AdapterError> {
        let (secret, _) = self.signing_key()?;
        let sig =
            hl_signing::sign_l1_action(&secret, action_mp, nonce).map_err(AdapterError::Other)?;
        let body = serde_json::json!({
            "action": action_json,
            "nonce": nonce,
            "signature": {"r": sig.r, "s": sig.s, "v": sig.v},
            "vaultAddress": null,
            "expiresAfter": null,
        });
        let resp = self
            .http
            .post(HL_EXCHANGE_URL)
            .json(&body)
            .timeout(WRITE_TIMEOUT)
            .send()
            .await
            .map_err(|e| AdapterError::UnknownOutcome(format!("下单请求未收到交易所响应：{e}")))?;
        if !resp.status().is_success() {
            return Err(AdapterError::UnknownOutcome(format!(
                "交易所返回 HTTP {}，订单状态未知",
                resp.status()
            )));
        }
        resp.json().await.map_err(|e| {
            AdapterError::UnknownOutcome(format!("交易所响应无法解析，订单状态未知：{e}"))
        })
    }
}

/// The `cancel` action, exactly as the official Python SDK builds it
/// (`Exchange.bulk_cancel`): short keys `a` (asset) / `o` (oid) — the
/// same wire shorthand as the `order` action. NOTE the asymmetry:
/// `cancelByCloid` uses the full names `asset` / `cloid` instead.
/// Both forms were re-verified against the freshly installed official
/// SDK on 2026-11-26 (msgpack bytes, action hash and r/s/v
/// byte-identical for the test key 0x11…11, nonce 1700000000000);
/// the tests below pin the adapter's construction to those vectors,
/// so a future key-name drift fails loudly instead of silently
/// leaving the order on the book.
pub(crate) fn cancel_action_mp(asset: u32, oid: u64) -> Mp<'static> {
    Mp::Map(vec![
        ("type", Mp::Str("cancel")),
        (
            "cancels",
            Mp::Array(vec![Mp::Map(vec![
                ("a", Mp::UInt(asset as u64)),
                ("o", Mp::UInt(oid)),
            ])]),
        ),
    ])
}

pub(crate) fn cancel_action_json(asset: u32, oid: u64) -> Value {
    serde_json::json!({
        "type": "cancel",
        "cancels": [{"a": asset, "o": oid}],
    })
}

pub(crate) fn cancel_by_cloid_action_mp<'a>(asset: u32, cloid: &'a str) -> Mp<'a> {
    Mp::Map(vec![
        ("type", Mp::Str("cancelByCloid")),
        (
            "cancels",
            Mp::Array(vec![Mp::Map(vec![
                ("asset", Mp::UInt(asset as u64)),
                ("cloid", Mp::Str(cloid)),
            ])]),
        ),
    ])
}

pub(crate) fn cancel_by_cloid_action_json(asset: u32, cloid: &str) -> Value {
    serde_json::json!({
        "type": "cancelByCloid",
        "cancels": [{"asset": asset, "cloid": cloid}],
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The adapter's cancel actions, pinned to the official SDK's
    /// vectors (re-generated from the freshly installed SDK on
    /// 2026-11-26, test key 0x11…11, nonce 1700000000000 — same
    /// fixtures as `hl_signing`'s golden-vector tests). This is the
    /// link the old tests missed: the signing was verified, but
    /// nothing checked that the adapter builds the same action the
    /// signer was tested with.
    #[test]
    fn cancel_actions_match_the_official_sdk() {
        let mut out = Vec::new();
        hl_signing::encode_msgpack(&cancel_action_mp(0, 77_738_308), &mut out);
        assert_eq!(
            hex::encode(&out),
            "82a474797065a663616e63656ca763616e63656c739182a16100a16fce04a23144"
        );
        let mut out = Vec::new();
        hl_signing::encode_msgpack(
            &cancel_by_cloid_action_mp(0, "0x1234567890abcdef1234567890abcdef"),
            &mut out,
        );
        assert_eq!(
            hex::encode(&out),
            "82a474797065ad63616e63656c4279436c6f6964a763616e63656c739182a5617373657400a5636c6f6964d92230783132333435363738393061626364656631323334353637383930616263646566"
        );
        // The JSON action posted to /exchange must use the same keys.
        assert_eq!(
            cancel_action_json(0, 77_738_308),
            serde_json::json!({"type": "cancel", "cancels": [{"a": 0, "o": 77738308}]})
        );
        assert_eq!(
            cancel_by_cloid_action_json(0, "0x1234567890abcdef1234567890abcdef"),
            serde_json::json!({"type": "cancelByCloid", "cancels": [{"asset": 0, "cloid": "0x1234567890abcdef1234567890abcdef"}]})
        );
    }
}
