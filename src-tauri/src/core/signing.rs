//! Hyperliquid L1 action signing (phase 5A §4) — pure Rust, no
//! alloy/ethers/msgpack crates.
//!
//! The construction is the official Python SDK's `sign_l1_action`
//! (`hyperliquid/utils/signing.py`, verified 2026-11-26 against the
//! SDK source and the API docs):
//!
//! ```text
//! action_hash = keccak256(msgpack(action) || nonce_be8 || 0x00)
//!     // vaultAddress is always None here (no vaults/subaccounts):
//!     // a 0x00 flag byte, nothing else. expires_after is None:
//!     // nothing appended.
//! phantom agent = { source: "a" (mainnet), connectionId: action_hash }
//! EIP-712: domain { name: "Exchange", version: "1", chainId: 1337,
//!          verifyingContract: 0x0 }
//!          type Agent(string source, bytes32 connectionId)
//! signature = { r, s, v } over the EIP-712 digest, v = 27/28.
//! ```
//!
//! The msgpack encoder below is a minimal hand-roll for the action
//! shapes only (maps, arrays, strings, uints, bools). Key order is
//! the SDK's insertion order and MUST be preserved — the server
//! re-encodes the action the same way, so a different order breaks
//! the signature. Every encoder output is covered by golden vectors
//! generated with the real SDK (same key, action and nonce) in the
//! tests at the bottom of this file.
//!
//! The signing key is the account's Agent/API wallet private key.
//! It never enters a log line or an error message.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use k256::ecdsa::signature::hazmat::PrehashSigner;
use k256::ecdsa::{RecoveryId, Signature, SigningKey, VerifyingKey};
use rand::Rng;
use sha3::{Digest, Keccak256};

use crate::core::util::now_ms;

/// EIP-712 domain chain id for L1 actions (NOT 42161 — the SDK's
/// `l1_payload` uses 1337).
const L1_CHAIN_ID: u64 = 1337;

/// Ordered msgpack value. Maps keep insertion order on purpose.
#[derive(Clone, Debug)]
pub enum Mp<'a> {
    Nil,
    Bool(bool),
    UInt(u64),
    Int(i64),
    Str(&'a str),
    Array(Vec<Mp<'a>>),
    Map(Vec<(&'a str, Mp<'a>)>),
}

/// Minimal msgpack encoder (spec: fixmap/fixarray/fixstr, uint
/// 8/16/32/64, int 8/16/32/64, str8/16/32, array16/32, map16/32).
/// Only the value kinds actions use are supported; anything else
/// (floats, binary) is a hard error, never silently mis-encoded.
pub fn encode_msgpack(v: &Mp, out: &mut Vec<u8>) {
    match v {
        Mp::Nil => out.push(0xc0),
        Mp::Bool(true) => out.push(0xc3),
        Mp::Bool(false) => out.push(0xc2),
        Mp::UInt(n) => encode_uint(*n, out),
        Mp::Int(n) => encode_int(*n, out),
        Mp::Str(s) => {
            let b = s.as_bytes();
            match b.len() {
                0..=31 => out.push(0xa0 | b.len() as u8),
                32..=255 => {
                    out.push(0xd9);
                    out.push(b.len() as u8);
                }
                256..=65535 => {
                    out.push(0xda);
                    out.extend_from_slice(&(b.len() as u16).to_be_bytes());
                }
                _ => {
                    out.push(0xdb);
                    out.extend_from_slice(&(b.len() as u32).to_be_bytes());
                }
            }
            out.extend_from_slice(b);
        }
        Mp::Array(items) => {
            match items.len() {
                0..=15 => out.push(0x90 | items.len() as u8),
                16..=65535 => {
                    out.push(0xdc);
                    out.extend_from_slice(&(items.len() as u16).to_be_bytes());
                }
                _ => {
                    out.push(0xdd);
                    out.extend_from_slice(&(items.len() as u32).to_be_bytes());
                }
            }
            for item in items {
                encode_msgpack(item, out);
            }
        }
        Mp::Map(entries) => {
            match entries.len() {
                0..=15 => out.push(0x80 | entries.len() as u8),
                16..=65535 => {
                    out.push(0xde);
                    out.extend_from_slice(&(entries.len() as u16).to_be_bytes());
                }
                _ => {
                    out.push(0xdf);
                    out.extend_from_slice(&(entries.len() as u32).to_be_bytes());
                }
            }
            for (k, val) in entries {
                encode_msgpack(&Mp::Str(k), out);
                encode_msgpack(val, out);
            }
        }
    }
}

fn encode_uint(n: u64, out: &mut Vec<u8>) {
    match n {
        0..=127 => out.push(n as u8),
        128..=255 => {
            out.push(0xcc);
            out.push(n as u8);
        }
        256..=65535 => {
            out.push(0xcd);
            out.extend_from_slice(&(n as u16).to_be_bytes());
        }
        65536..=4_294_967_295 => {
            out.push(0xce);
            out.extend_from_slice(&(n as u32).to_be_bytes());
        }
        _ => {
            out.push(0xcf);
            out.extend_from_slice(&n.to_be_bytes());
        }
    }
}

fn encode_int(n: i64, out: &mut Vec<u8>) {
    if n >= 0 {
        encode_uint(n as u64, out);
        return;
    }
    match n {
        -32..=-1 => out.push(0xe0 | (n + 32) as u8),
        -128..=-33 => {
            out.push(0xd0);
            out.push(n as u8);
        }
        -32768..=-129 => {
            out.push(0xd1);
            out.extend_from_slice(&(n as i16).to_be_bytes());
        }
        -2_147_483_648..=-32_769 => {
            out.push(0xd2);
            out.extend_from_slice(&(n as i32).to_be_bytes());
        }
        _ => {
            out.push(0xd3);
            out.extend_from_slice(&n.to_be_bytes());
        }
    }
}

fn keccak256(data: &[u8]) -> [u8; 32] {
    let mut h = Keccak256::new();
    h.update(data);
    h.finalize().into()
}

/// `action_hash` from the SDK: keccak256(msgpack(action) || nonce
/// (8 bytes big-endian) || 0x00). The trailing 0x00 is the
/// vault-address flag for `vaultAddress = None`; no `expires_after`
/// is ever appended (we never set one).
pub fn action_hash(action: &Mp, nonce: u64) -> [u8; 32] {
    let mut data = Vec::new();
    encode_msgpack(action, &mut data);
    data.extend_from_slice(&nonce.to_be_bytes());
    data.push(0x00);
    keccak256(&data)
}

/// EIP-712 digest of the phantom agent `{ source: "a", connectionId:
/// hash }` under domain Exchange/1/1337/0x0.
fn l1_digest(hash: [u8; 32]) -> [u8; 32] {
    let domain_type = keccak256(
        b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
    );
    let mut enc = Vec::with_capacity(32 * 5);
    enc.extend_from_slice(&domain_type);
    enc.extend_from_slice(&keccak256(b"Exchange"));
    enc.extend_from_slice(&keccak256(b"1"));
    // chainId is a uint256: 32 bytes, big-endian, left-padded.
    let mut chain = [0u8; 32];
    chain[24..32].copy_from_slice(&L1_CHAIN_ID.to_be_bytes());
    enc.extend_from_slice(&chain);
    enc.extend_from_slice(&[0u8; 32]); // verifyingContract = 0x0
    let domain_sep = keccak256(&enc);

    let agent_type = keccak256(b"Agent(string source,bytes32 connectionId)");
    let mut s = Vec::with_capacity(32 * 3);
    s.extend_from_slice(&agent_type);
    s.extend_from_slice(&keccak256(b"a")); // source = "a" (mainnet)
    s.extend_from_slice(&hash);
    let struct_hash = keccak256(&s);

    let mut d = Vec::with_capacity(66);
    d.extend_from_slice(&[0x19, 0x01]);
    d.extend_from_slice(&domain_sep);
    d.extend_from_slice(&struct_hash);
    keccak256(&d)
}

/// The `/exchange` signature object: r/s as `0x`-prefixed 32-byte
/// hex, v as 27/28.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HlSignature {
    pub r: String,
    pub s: String,
    pub v: u8,
}

/// Sign an L1 action with an agent private key. Deterministic
/// (RFC 6979) ECDSA, so the same key + action + nonce always yields
/// the same signature — the golden-vector tests rely on that.
pub fn sign_l1_action(secret: &[u8; 32], action: &Mp, nonce: u64) -> Result<HlSignature, String> {
    let sk = SigningKey::from_bytes((&secret[..]).into())
        .map_err(|_| "Agent 钱包私钥格式不正确".to_string())?;
    let digest = l1_digest(action_hash(action, nonce));
    let sig: Signature = sk
        .sign_prehash(&digest)
        .map_err(|e| format!("EIP-712 签名失败：{e}"))?;
    // Trial recovery: the id whose recovered key matches the signer.
    for id in [0u8, 1] {
        let recid = RecoveryId::from_byte(id).ok_or_else(|| "EIP-712 签名失败".to_string())?;
        if let Ok(vk) = VerifyingKey::recover_from_prehash(&digest, &sig, recid) {
            if vk == *sk.verifying_key() {
                let b = sig.to_bytes();
                return Ok(HlSignature {
                    r: format!("0x{}", hex::encode(&b[..32])),
                    s: format!("0x{}", hex::encode(&b[32..])),
                    v: id + 27,
                });
            }
        }
    }
    Err("EIP-712 签名失败：无法确定 recovery id".to_string())
}

/// One globally shared, strictly increasing nonce per signing
/// address (task book §4). Hyperliquid and Entropy share the same
/// signing address space, so a single allocator for both is what
/// keeps their nonces from colliding. `next` never returns a value
/// below the current wall-clock millisecond, and never repeats.
pub struct NonceAllocator;

static NONCE_LAST: OnceLock<Mutex<HashMap<String, u64>>> = OnceLock::new();

fn nonce_last() -> &'static Mutex<HashMap<String, u64>> {
    NONCE_LAST.get_or_init(|| Mutex::new(HashMap::new()))
}

impl NonceAllocator {
    pub fn next(address: &str) -> u64 {
        Self::next_at(address, now_ms() as u64)
    }

    /// Testable core: `now` is injected so tests are deterministic.
    pub fn next_at(address: &str, now: u64) -> u64 {
        let mut map = nonce_last().lock().unwrap_or_else(|e| e.into_inner());
        let last = map.get(address).copied().unwrap_or(0);
        let next = now.max(last.saturating_add(1));
        map.insert(address.to_string(), next);
        next
    }
}

/// A fresh client order id: 16 random bytes as `0x` + 32 hex chars
/// (the docs' cloid format). Unique per order; the exchange rejects
/// a reused cloid.
pub fn generate_cloid() -> String {
    let mut rng = rand::thread_rng();
    let mut bytes = [0u8; 16];
    rng.fill(&mut bytes);
    format!("0x{}", hex::encode(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Fixed test key (never a real key). Its address, per the SDK:
    /// 0x19e7e376e7c213b7e7e7e46cc70a5dd086daff2a.
    fn test_secret() -> [u8; 32] {
        [0x11u8; 32]
    }

    const NONCE: u64 = 1_700_000_000_000;

    /// The order action from the golden-vector run, in SDK key order.
    fn order_action<'a>() -> Mp<'a> {
        Mp::Map(vec![
            ("type", Mp::Str("order")),
            (
                "orders",
                Mp::Array(vec![Mp::Map(vec![
                    ("a", Mp::UInt(0)),
                    ("b", Mp::Bool(true)),
                    ("p", Mp::Str("90000.0")),
                    ("s", Mp::Str("0.01")),
                    ("r", Mp::Bool(false)),
                    (
                        "t",
                        Mp::Map(vec![("limit", Mp::Map(vec![("tif", Mp::Str("Gtc"))]))]),
                    ),
                    ("c", Mp::Str("0x1234567890abcdef1234567890abcdef")),
                ])]),
            ),
            ("grouping", Mp::Str("na")),
        ])
    }

    fn cancel_action<'a>() -> Mp<'a> {
        Mp::Map(vec![
            ("type", Mp::Str("cancel")),
            (
                "cancels",
                Mp::Array(vec![Mp::Map(vec![
                    ("a", Mp::UInt(0)),
                    ("o", Mp::UInt(77_738_308)),
                ])]),
            ),
        ])
    }

    fn cancel_by_cloid_action<'a>() -> Mp<'a> {
        Mp::Map(vec![
            ("type", Mp::Str("cancelByCloid")),
            (
                "cancels",
                Mp::Array(vec![Mp::Map(vec![
                    ("asset", Mp::UInt(0)),
                    ("cloid", Mp::Str("0x1234567890abcdef1234567890abcdef")),
                ])]),
            ),
        ])
    }

    fn pack_hex(v: &Mp) -> String {
        let mut out = Vec::new();
        encode_msgpack(v, &mut out);
        hex::encode(out)
    }

    #[test]
    fn msgpack_matches_the_official_sdk() {
        assert_eq!(
            pack_hex(&order_action()),
            "83a474797065a56f72646572a66f72646572739187a16100a162c3a170a739303030302e30a173a4302e3031a172c2a17481a56c696d697481a3746966a3477463a163d92230783132333435363738393061626364656631323334353637383930616263646566a867726f7570696e67a26e61"
        );
        assert_eq!(
            pack_hex(&cancel_action()),
            "82a474797065a663616e63656ca763616e63656c739182a16100a16fce04a23144"
        );
        assert_eq!(
            pack_hex(&cancel_by_cloid_action()),
            "82a474797065ad63616e63656c4279436c6f6964a763616e63656c739182a5617373657400a5636c6f6964d92230783132333435363738393061626364656631323334353637383930616263646566"
        );
        // Spec spot checks: fixmap/fixstr/fixint boundaries.
        assert_eq!(pack_hex(&Mp::Map(vec![("a", Mp::UInt(1))])), "81a16101");
        assert_eq!(pack_hex(&Mp::UInt(127)), "7f");
        assert_eq!(pack_hex(&Mp::UInt(128)), "cc80");
        assert_eq!(pack_hex(&Mp::Int(-1)), "ff");
        assert_eq!(pack_hex(&Mp::Int(-33)), "d0df");
        assert_eq!(pack_hex(&Mp::Bool(true)), "c3");
        assert_eq!(pack_hex(&Mp::Nil), "c0");
    }

    #[test]
    fn action_hash_matches_the_official_sdk() {
        assert_eq!(
            hex::encode(action_hash(&order_action(), NONCE)),
            "29241af242754b51f84a0228d1b9b805cfe407ebe493f6e27b726e963b467bd5"
        );
        assert_eq!(
            hex::encode(action_hash(&cancel_action(), NONCE)),
            "99f95e9c3ec4a58b28dd8ea91529b4d305494e7601c1b6e6fe631fac3168c298"
        );
        assert_eq!(
            hex::encode(action_hash(&cancel_by_cloid_action(), NONCE)),
            "435c79a1720d068d43b842b0e6b5386afe432bf4d9eb2950dd4c07b67cef7322"
        );
    }

    #[test]
    fn l1_signature_matches_the_official_sdk() {
        let sig = sign_l1_action(&test_secret(), &order_action(), NONCE).unwrap();
        assert_eq!(
            sig.r,
            "0xd43bd50d14c86605409fedd7c078cbf2dc45a6e338cee0f5ff8bddcd31a1eca0"
        );
        assert_eq!(
            sig.s,
            // The SDK's `to_hex` strips the leading zero nibble; the
            // number is the same, ours is the full 32-byte form.
            "0x0fad9c72b5f4dbc1040176a2212e749cedcd2d18e3046260dc7a3c0bffc22698"
        );
        assert_eq!(sig.v, 28);

        let sig = sign_l1_action(&test_secret(), &cancel_action(), NONCE).unwrap();
        assert_eq!(
            sig.r,
            "0x3986d56d1ab47591cb36a259d0d3d912538e9dac40ce0971da65ebcef560c138"
        );
        assert_eq!(
            sig.s,
            "0x2fc8ac319585fe8df01871cd7b4e62b389614bf2510b0e0c7fc9aea0abd22d3c"
        );
        assert_eq!(sig.v, 28);

        let sig = sign_l1_action(&test_secret(), &cancel_by_cloid_action(), NONCE).unwrap();
        assert_eq!(
            sig.r,
            "0x3076116ebf4f6f1b5cab4c1a85db2ab4e1a178c539ccfa3ce485466e0f346cbd"
        );
        assert_eq!(
            sig.s,
            "0x796b0c80874293d153546a099cfc5b7c1f6d5a1367fd7fbf482d2bfef2ebb079"
        );
        assert_eq!(sig.v, 27);
    }

    #[test]
    fn nonce_allocator_is_strictly_increasing_under_concurrency() {
        let addr = "0xnonce-test-concurrent";
        let mut handles = Vec::new();
        for _ in 0..8 {
            handles.push(std::thread::spawn(move || {
                (0..100)
                    .map(|_| NonceAllocator::next(addr))
                    .collect::<Vec<u64>>()
            }));
        }
        let mut all: Vec<u64> = Vec::new();
        for h in handles {
            all.extend(h.join().unwrap());
        }
        all.sort_unstable();
        assert_eq!(all.len(), 800);
        for w in all.windows(2) {
            assert!(w[1] > w[0], "nonce repeated or went backwards");
        }
    }

    #[test]
    fn nonce_allocator_never_goes_below_wall_clock() {
        let addr = "0xnonce-test-clock";
        let a = NonceAllocator::next_at(addr, 1_000);
        let b = NonceAllocator::next_at(addr, 500);
        assert_eq!(a, 1_000);
        assert_eq!(b, 1_001, "a smaller `now` must still move forward");
        let c = NonceAllocator::next_at("0xnonce-test-other", 500);
        assert_eq!(c, 500, "different addresses are independent");
    }

    #[test]
    fn cloid_format_is_128_bit_hex() {
        let c = generate_cloid();
        assert!(c.starts_with("0x"));
        assert_eq!(c.len(), 34);
        assert!(c[2..].chars().all(|ch| ch.is_ascii_hexdigit()));
        assert_ne!(generate_cloid(), generate_cloid());
    }
}
