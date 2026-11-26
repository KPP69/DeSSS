//! AsterDEX V3 EIP-712 signing (Agent-wallet mode). Moved verbatim
//! from `adapters/asterdex.rs` (PR-1.3 commit 2).
//!
//! Official V3 scheme (asterdex/api-docs, V3 recommended): the signed
//! message is the URL-encoded request parameter string itself, signed
//! as the single `msg` field of an EIP-712 `Message` struct. Domain:
//! name "AsterSignTransaction", version "1", chainId 1666 (mainnet),
//! verifyingContract 0x0. The signature is 65 bytes r || s || v
//! (v = 27/28), lowercase hex, no 0x prefix. Pure Rust (`k256` +
//! `sha3`); the agent secret never enters a log line or error message.

use k256::ecdsa::signature::hazmat::PrehashSigner;
use sha3::{Digest, Keccak256};

/// V3 EIP-712 domain chain id (mainnet; testnet uses 714).
const V3_CHAIN_ID: u16 = 1666;

pub(crate) fn keccak256(data: &[u8]) -> [u8; 32] {
    let mut h = Keccak256::new();
    h.update(data);
    h.finalize().into()
}

/// EIP-712 digest of the V3 `Message(string msg)` struct for `msg`.
pub(crate) fn eip712_digest(msg: &str) -> [u8; 32] {
    let domain_type = keccak256(
        b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
    );
    let mut enc = Vec::with_capacity(32 * 5);
    enc.extend_from_slice(&domain_type);
    enc.extend_from_slice(&keccak256(b"AsterSignTransaction"));
    enc.extend_from_slice(&keccak256(b"1"));
    let mut chain = [0u8; 32];
    chain[30..32].copy_from_slice(&V3_CHAIN_ID.to_be_bytes());
    enc.extend_from_slice(&chain);
    enc.extend_from_slice(&[0u8; 32]); // verifyingContract = 0x0
    let domain_sep = keccak256(&enc);

    let msg_type = keccak256(b"Message(string msg)");
    let mut s = Vec::with_capacity(64);
    s.extend_from_slice(&msg_type);
    s.extend_from_slice(&keccak256(msg.as_bytes()));
    let struct_hash = keccak256(&s);

    let mut d = Vec::with_capacity(66);
    d.extend_from_slice(&[0x19, 0x01]);
    d.extend_from_slice(&domain_sep);
    d.extend_from_slice(&struct_hash);
    keccak256(&d)
}

/// Decode an agent private key (64 hex chars, optional 0x prefix).
pub fn decode_agent_secret(hex_str: &str) -> Result<[u8; 32], String> {
    let s = hex_str.strip_prefix("0x").unwrap_or(hex_str);
    let bytes =
        hex::decode(s).map_err(|_| "Agent 钱包私钥格式不正确（应为 64 位十六进制）".to_string())?;
    bytes
        .try_into()
        .map_err(|_| "Agent 钱包私钥格式不正确（应为 64 位十六进制）".to_string())
}

/// Ethereum address of an agent private key (lowercase, no checksum).
pub fn address_from_secret(secret: &[u8; 32]) -> String {
    let sk = k256::ecdsa::SigningKey::from_bytes((&secret[..]).into())
        .expect("a validated 32-byte secret is a valid key");
    let pt = sk.verifying_key().to_encoded_point(false);
    let hash = keccak256(&pt.as_bytes()[1..]);
    format!("0x{}", hex::encode(&hash[12..]))
}

/// Sign a V3 parameter string with the agent private key.
pub(crate) fn sign_v3(secret: &[u8; 32], msg: &str) -> Result<String, String> {
    let sk = k256::ecdsa::SigningKey::from_bytes((&secret[..]).into())
        .map_err(|_| "Agent 钱包私钥格式不正确".to_string())?;
    let digest = eip712_digest(msg);
    let sig: k256::ecdsa::Signature = sk
        .sign_prehash(&digest)
        .map_err(|e| format!("EIP-712 签名失败：{e}"))?;
    // Trial recovery: the id whose recovered key matches the signer.
    for id in [0u8, 1] {
        let recid =
            k256::ecdsa::RecoveryId::from_byte(id).ok_or_else(|| "EIP-712 签名失败".to_string())?;
        if let Ok(vk) = k256::ecdsa::VerifyingKey::recover_from_prehash(&digest, &sig, recid) {
            if vk == *sk.verifying_key() {
                let mut out = sig.to_bytes().to_vec();
                out.push(id + 27);
                return Ok(hex::encode(out));
            }
        }
    }
    Err("EIP-712 签名失败：无法确定 recovery id".to_string())
}

/// Next V3 nonce: microsecond timestamp, strictly increasing (the server
/// rejects reused nonces, so a repeat timestamp is bumped by one).
pub(crate) fn next_nonce_value(last: u128, now: u128) -> u128 {
    now.max(last + 1)
}
