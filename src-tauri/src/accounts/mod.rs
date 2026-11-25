//! Account service layer: metadata in SQLite, credentials in the vault.
//!
//! Rules enforced here (not just in the frontend):
//! - An account's `exchange_id` is locked at creation; `update_account`
//!   re-reads the stored value and rejects any change.
//! - Credentials are validated per exchange before anything is persisted.
//! - Empty/absent credential input on update means "keep the old ones".
//! - Create/update/delete keep SQLite and the vault consistent: if the
//!   second write fails, the first one is rolled back.
//! - Nothing returned from this module ever contains a full credential.

use serde::Serialize;
use serde_json::{Map, Value};

use crate::data_engine::{AccountRow, DataEngine};
use crate::vault::{VaultError, VaultStore};

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountView {
    pub id: String,
    pub label: String,
    pub exchange_id: String,
    pub masked_identifier: String,
    pub created_at: i64,
    pub updated_at: i64,
    pub status: String,
    pub credential_configured: bool,
}

impl From<&AccountRow> for AccountView {
    fn from(row: &AccountRow) -> Self {
        Self {
            id: row.id.clone(),
            label: row.label.clone(),
            exchange_id: row.exchange_id.clone(),
            masked_identifier: row.masked_identifier.clone(),
            created_at: row.created_at,
            updated_at: row.updated_at,
            status: row.status.clone(),
            credential_configured: row.credential_configured,
        }
    }
}

pub const SUPPORTED_EXCHANGES: [&str; 6] = [
    "hyperliquid",
    "entropy",
    "lighter",
    "lighter_robinhood",
    "asterdex",
    "arcus",
];

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn field_str<'a>(creds: &'a Map<String, Value>, key: &str) -> Option<&'a str> {
    creds
        .get(key)
        .and_then(|v| v.as_str())
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
}

fn require_str(creds: &Map<String, Value>, key: &str, label: &str) -> Result<String, String> {
    field_str(creds, key)
        .map(|s| s.to_string())
        .ok_or_else(|| format!("缺少必填项：{label}"))
}

/// Index fields may arrive as JSON numbers or numeric strings.
fn field_index(creds: &Map<String, Value>, key: &str) -> Option<i64> {
    match creds.get(key) {
        Some(Value::Number(n)) => n.as_i64(),
        Some(Value::String(s)) => s.trim().parse::<i64>().ok(),
        _ => None,
    }
}

fn require_index(creds: &Map<String, Value>, key: &str, label: &str) -> Result<i64, String> {
    field_index(creds, key).ok_or_else(|| format!("字段缺失或无效：{label}"))
}

fn mask_address(addr: &str) -> String {
    let chars: Vec<char> = addr.chars().collect();
    if chars.len() > 10 {
        let head: String = chars.iter().take(6).collect();
        let tail: String = chars.iter().skip(chars.len() - 4).collect();
        format!("{head}…{tail}")
    } else {
        "••••••".to_string()
    }
}

fn mask_api_key(key: &str) -> String {
    let chars: Vec<char> = key.chars().collect();
    if chars.len() > 8 {
        let head: String = chars.iter().take(4).collect();
        let tail: String = chars.iter().skip(chars.len() - 4).collect();
        format!("{head}…{tail}")
    } else {
        "••••••".to_string()
    }
}

/// Validate a full credential set for `exchange_id` and derive the masked
/// identifier stored in SQLite. Error messages name fields, never values.
pub fn validate_credentials(
    exchange_id: &str,
    creds: &Map<String, Value>,
) -> Result<String, String> {
    match exchange_id {
        // Entropy reuses the Hyperliquid HIP-3 access pattern: same fields.
        "hyperliquid" | "entropy" => {
            let wallet = require_str(creds, "wallet_address", "主钱包地址")?;
            require_str(creds, "agent_private_key", "Agent/API 钱包私钥")?;
            Ok(mask_address(&wallet))
        }
        "lighter" => {
            let account_index = require_index(creds, "account_index", "Account Index")?;
            let api_key_index = require_index(creds, "api_key_index", "API Key Index")?;
            if !(4..=254).contains(&api_key_index) {
                return Err("Lighter API Key Index 须在 4–254 之间".to_string());
            }
            require_str(creds, "api_key_private_key", "API Key 私钥")?;
            Ok(format!(
                "Account #{account_index} · API key #{api_key_index}"
            ))
        }
        "lighter_robinhood" => {
            let account_index = require_index(creds, "account_index", "Account Index")?;
            let api_key_index = require_index(creds, "api_key_index", "API Key Index")?;
            if api_key_index == 157 {
                return Err("Lighter RH 的 API Key Index 157 已保留，不可用".to_string());
            }
            if !(4..=156).contains(&api_key_index) && !(158..=254).contains(&api_key_index) {
                return Err("Lighter RH API Key Index 须为 4–156 或 158–254".to_string());
            }
            require_str(creds, "api_key_private_key", "API Key 私钥")?;
            Ok(format!(
                "Account #{account_index} · API key #{api_key_index}"
            ))
        }
        "asterdex" => {
            let mode = field_str(creds, "mode").unwrap_or("api");
            match mode {
                "agent" => {
                    // V3 signing needs the master wallet (user) as well as
                    // the agent address (signer) and the agent private key.
                    let master = require_str(creds, "master_wallet_address", "主钱包地址")?;
                    require_str(creds, "agent_wallet_address", "Agent 钱包地址")?;
                    require_str(creds, "agent_private_key", "Agent 钱包私钥")?;
                    Ok(mask_address(&master))
                }
                _ => {
                    let api_key = require_str(creds, "api_key", "API Key")?;
                    require_str(creds, "api_secret", "API Secret")?;
                    Ok(mask_api_key(&api_key))
                }
            }
        }
        "arcus" => {
            let wallet = require_str(creds, "wallet_address", "钱包地址")?;
            require_str(creds, "signing_private_key", "交易签名私钥/凭证")?;
            Ok(mask_address(&wallet))
        }
        other => Err(format!("不支持的交易所：{other}")),
    }
}

fn validate_label(label: &str) -> Result<String, String> {
    let label = label.trim();
    if label.is_empty() {
        return Err("账户名不能为空".to_string());
    }
    if label.chars().count() > 64 {
        return Err("账户名过长（最多 64 个字符）".to_string());
    }
    Ok(label.to_string())
}

/// True when the map carries at least one non-empty credential value,
/// i.e. the user actually typed new credentials in the edit form.
fn has_new_credentials(creds: &Map<String, Value>) -> bool {
    creds.values().any(|v| match v {
        Value::String(s) => !s.trim().is_empty(),
        Value::Number(n) => n.as_i64().is_some() || n.as_f64().is_some(),
        _ => false,
    })
}

/// Combine a primary failure with a rollback outcome. A failed rollback is
/// never swallowed: the caller gets both errors in one message so a
/// SQLite/vault divergence is visible to the user instead of silent.
fn with_rollback_error(main_err: String, rollback: Result<(), VaultError>) -> String {
    match rollback {
        Ok(()) => main_err,
        Err(rb) => format!("{main_err} （回滚也失败：{rb}）"),
    }
}

pub fn list_accounts(db: &DataEngine) -> Result<Vec<AccountView>, String> {
    Ok(db
        .list_accounts()?
        .iter()
        .map(AccountView::from)
        .collect())
}

pub fn create_account(
    db: &DataEngine,
    vault: &VaultStore,
    label: &str,
    exchange_id: &str,
    credentials: Map<String, Value>,
) -> Result<AccountView, String> {
    let label = validate_label(label)?;
    if !SUPPORTED_EXCHANGES.contains(&exchange_id) {
        return Err(format!("不支持的交易所：{exchange_id}"));
    }
    let masked = validate_credentials(exchange_id, &credentials)?;
    let id = uuid::Uuid::new_v4().to_string();
    let now = now_ms();
    // Vault first: if the DB insert then fails, the vault entry is removed
    // again so the two stores never disagree.
    vault
        .set(&id, Value::Object(credentials))
        .map_err(|e| format!("保存凭证失败：{e}"))?;
    let row = AccountRow {
        id: id.clone(),
        label,
        exchange_id: exchange_id.to_string(),
        masked_identifier: masked,
        created_at: now,
        updated_at: now,
        status: "configured".to_string(),
        credential_configured: true,
    };
    if let Err(e) = db.insert_account(&row) {
        return Err(with_rollback_error(e, vault.remove(&id).map(|_| ())));
    }
    Ok(AccountView::from(&row))
}

pub fn update_account(
    db: &DataEngine,
    vault: &VaultStore,
    id: &str,
    label: &str,
    exchange_id: &str,
    credentials: Option<Map<String, Value>>,
) -> Result<AccountView, String> {
    let existing = db
        .get_account(id)?
        .ok_or_else(|| "找不到该账户".to_string())?;
    // Exchange lock, enforced on the server side: the requested exchange
    // must match what is already stored, no matter what the caller sends.
    if exchange_id != existing.exchange_id {
        return Err(
            "the exchange of an existing account cannot be changed; delete the account and create a new one to switch exchanges"
                .to_string(),
        );
    }
    let label = validate_label(label)?;
    let now = now_ms();

    let replacing = credentials
        .as_ref()
        .map(|c| has_new_credentials(c))
        .unwrap_or(false);

    if replacing {
        let creds = credentials.unwrap();
        let masked = validate_credentials(&existing.exchange_id, &creds)?;
        let old_creds = vault.get(id);
        vault
            .set(id, Value::Object(creds))
            .map_err(|e| format!("保存凭证失败：{e}"))?;
        if let Err(e) = db.update_account_meta(id, &label, &masked, true, now) {
            // Roll the vault back to the previous credentials.
            let rollback = match old_creds {
                Some(old) => vault.set(id, old),
                None => vault.remove(id).map(|_| ()),
            };
            return Err(with_rollback_error(e, rollback));
        }
        let mut view = AccountView::from(&existing);
        view.label = label;
        view.masked_identifier = masked;
        view.credential_configured = true;
        view.updated_at = now;
        Ok(view)
    } else {
        // Empty credential fields mean: keep the stored credentials as-is.
        db.update_account_meta(
            id,
            &label,
            &existing.masked_identifier,
            existing.credential_configured,
            now,
        )?;
        let mut view = AccountView::from(&existing);
        view.label = label;
        view.updated_at = now;
        Ok(view)
    }
}

pub fn delete_account(db: &DataEngine, vault: &VaultStore, id: &str) -> Result<(), String> {
    let _existing = db
        .get_account(id)?
        .ok_or_else(|| "找不到该账户".to_string())?;
    // Remove the vault entry first; if the DB delete then fails, restore it.
    let old_creds = vault.remove(id).map_err(|e| format!("删除凭证失败：{e}"))?;
    if let Err(e) = db.delete_account(id) {
        let rollback = match old_creds {
            Some(old) => vault.set(id, old),
            None => Ok(()),
        };
        return Err(with_rollback_error(e, rollback));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::portable::PortablePaths;
    use std::path::PathBuf;

    fn temp_paths(tag: &str) -> PortablePaths {
        let root = std::env::temp_dir().join(format!(
            "desss-accounts-test-{}-{}",
            std::process::id(),
            tag
        ));
        let _ = std::fs::remove_dir_all(&root);
        let data_dir = root.join("data");
        PortablePaths {
            db_path: data_dir.join("desss.db"),
            log_dir: data_dir.join("logs"),
            vault_key_path: data_dir.join("vault.key"),
            vault_enc_path: data_dir.join("vault.enc"),
            root,
            data_dir,
        }
    }

    fn hl_creds() -> Map<String, Value> {
        serde_json::json!({
            "wallet_address": "0x1234567890abcdef1234567890abcdef12345678",
            "agent_private_key": "0xdeadbeef"
        })
        .as_object()
        .unwrap()
        .clone()
    }

    fn setup(tag: &str) -> (DataEngine, VaultStore) {
        let paths = temp_paths(tag);
        let db = DataEngine::open(paths.clone()).expect("db");
        let vault = VaultStore::open(&paths).expect("vault");
        (db, vault)
    }

    #[test]
    fn create_list_and_reopen_roundtrip() {
        let paths = temp_paths("crud");
        let db = DataEngine::open(paths.clone()).expect("db");
        let vault = VaultStore::open(&paths).expect("vault");
        let view = create_account(&db, &vault, "Main HL", "hyperliquid", hl_creds()).expect("create");
        assert!(view.credential_configured);
        assert!(view.masked_identifier.contains('…'));
        assert!(!view.masked_identifier.contains("deadbeef"));
        drop(db);
        drop(vault);
        // Restart: metadata and credentials are both recovered.
        let db2 = DataEngine::open(paths.clone()).expect("db2");
        let vault2 = VaultStore::open(&paths).expect("vault2");
        let list = list_accounts(&db2).expect("list");
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].id, view.id);
        assert!(vault2.has(&view.id));
    }

    #[test]
    fn exchange_change_is_rejected_by_the_backend() {
        let (db, vault) = setup("lock");
        let view = create_account(&db, &vault, "A", "hyperliquid", hl_creds()).expect("create");
        let err = update_account(&db, &vault, &view.id, "A", "lighter", None)
            .expect_err("must reject exchange change");
        assert!(err.contains("cannot be changed"));
        // And the stored row is untouched.
        let row = db.get_account(&view.id).unwrap().unwrap();
        assert_eq!(row.exchange_id, "hyperliquid");
    }

    #[test]
    fn empty_credentials_on_update_keep_the_old_ones() {
        let (db, vault) = setup("keep");
        let view = create_account(&db, &vault, "A", "hyperliquid", hl_creds()).expect("create");
        let before = vault.get(&view.id);
        let empty = serde_json::json!({"wallet_address": "", "agent_private_key": ""})
            .as_object()
            .unwrap()
            .clone();
        update_account(&db, &vault, &view.id, "Renamed", "hyperliquid", Some(empty))
            .expect("update");
        assert_eq!(vault.get(&view.id), before);
        let row = db.get_account(&view.id).unwrap().unwrap();
        assert_eq!(row.label, "Renamed");
    }

    #[test]
    fn new_credentials_replace_the_old_ones() {
        let (db, vault) = setup("replace");
        let view = create_account(&db, &vault, "A", "hyperliquid", hl_creds()).expect("create");
        let mut new_creds = hl_creds();
        new_creds.insert(
            "agent_private_key".to_string(),
            Value::String("0xnewkey".to_string()),
        );
        update_account(&db, &vault, &view.id, "A", "hyperliquid", Some(new_creds)).expect("update");
        let stored = vault.get(&view.id).unwrap();
        assert_eq!(stored["agent_private_key"], Value::String("0xnewkey".to_string()));
    }

    #[test]
    fn delete_removes_db_row_and_vault_entry() {
        let paths = temp_paths("delete");
        let db = DataEngine::open(paths.clone()).expect("db");
        let vault = VaultStore::open(&paths).expect("vault");
        let view = create_account(&db, &vault, "A", "hyperliquid", hl_creds()).expect("create");
        delete_account(&db, &vault, &view.id).expect("delete");
        assert!(db.get_account(&view.id).unwrap().is_none());
        assert!(!vault.has(&view.id));
        drop(db);
        drop(vault);
        let db2 = DataEngine::open(paths.clone()).expect("db2");
        let vault2 = VaultStore::open(&paths).expect("vault2");
        assert_eq!(list_accounts(&db2).unwrap().len(), 0);
        assert!(!vault2.has(&view.id));
    }

    #[test]
    fn lighter_robinhood_rejects_reserved_index_157() {
        let creds = serde_json::json!({
            "account_index": 1, "api_key_index": 157, "api_key_private_key": "0xkey"
        })
        .as_object()
        .unwrap()
        .clone();
        let err = validate_credentials("lighter_robinhood", &creds).expect_err("157 reserved");
        assert!(err.contains("157"));
        let ok = serde_json::json!({
            "account_index": 1, "api_key_index": 158, "api_key_private_key": "0xkey"
        })
        .as_object()
        .unwrap()
        .clone();
        assert!(validate_credentials("lighter_robinhood", &ok).is_ok());
    }

    #[test]
    fn missing_required_fields_are_rejected() {
        let creds = serde_json::json!({"wallet_address": "0x1234567890abcdef"})
            .as_object()
            .unwrap()
            .clone();
        assert!(validate_credentials("hyperliquid", &creds).is_err());
        assert!(validate_credentials("unknown-exchange", &creds).is_err());
    }

    #[test]
    fn sqlite_never_contains_secrets() {
        let paths = temp_paths("no-secrets");
        let db = DataEngine::open(paths.clone()).expect("db");
        let vault = VaultStore::open(&paths).expect("vault");
        create_account(&db, &vault, "A", "hyperliquid", hl_creds()).expect("create");
        drop(db);
        let raw = std::fs::read(&paths.db_path).unwrap();
        let text = String::from_utf8_lossy(&raw);
        assert!(!text.contains("deadbeef"), "private key leaked into SQLite");
        let key_bytes = std::fs::read(&paths.vault_key_path).unwrap();
        assert_ne!(key_bytes.len(), 0);
        let _ = PathBuf::new();
    }

    #[test]
    fn rollback_error_keeps_main_error_when_rollback_succeeds() {
        let msg = with_rollback_error("db insert failed".to_string(), Ok(()));
        assert_eq!(msg, "db insert failed");
    }

    #[test]
    fn rollback_error_reports_failed_rollback() {
        let msg = with_rollback_error(
            "db insert failed".to_string(),
            Err(VaultError::MissingKeyWithData),
        );
        assert!(msg.contains("db insert failed"));
        assert!(msg.contains("回滚也失败"));
        assert!(msg.contains("vault.key 缺失"));
    }
}
