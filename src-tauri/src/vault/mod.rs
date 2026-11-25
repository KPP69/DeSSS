//! Portable local credential vault (phase 2).
//!
//! Layout — everything lives in the exe-adjacent `./data` directory:
//! - `vault.key`: 32 random bytes, generated once on first start (0600 on Unix).
//! - `vault.enc`: `nonce(12) || AES-256-GCM ciphertext`, where the plaintext is
//!   a JSON object mapping account id -> credential object.
//!
//! The key and the ciphertext sit in the same folder by design (portable,
//! copy-the-folder-and-it-works). This protects against plaintext exposure,
//! not against an attacker who obtains both files — an accepted trade-off.
//!
//! Safety rules enforced here:
//! - If `vault.enc` exists but `vault.key` is missing, refuse to start the
//!   vault (never generate a new key and orphan existing data).
//! - If decryption fails, return an error and leave the file untouched
//!   (never silently rebuild or overwrite).
//! - Every encryption uses a fresh CSPRNG nonce.
//! - Writes go to a temp file and are atomically renamed over the target.
//! - Key material never appears in logs, errors, SQLite or the frontend.

use std::collections::HashMap;
use std::io::Write;
use std::path::Path;
use std::sync::Mutex;

use aes_gcm::aead::Aead;
use aes_gcm::{Aes256Gcm, Key, KeyInit, Nonce};
use rand::rngs::OsRng;
use rand::RngCore;
use serde_json::Value;

use crate::portable::PortablePaths;

const KEY_LEN: usize = 32;
const NONCE_LEN: usize = 12;
/// Smallest valid vault.enc: nonce + GCM tag (16) with empty plaintext.
const MIN_ENC_LEN: usize = NONCE_LEN + 16;

#[derive(Debug, thiserror::Error)]
pub enum VaultError {
    #[error("vault.key 缺失，但 vault.enc 存在；拒绝生成新密钥，否则已有加密凭证将无法恢复")]
    MissingKeyWithData,
    #[error("vault.key 长度不正确（{0} 字节，应为 32 字节），文件可能已损坏")]
    InvalidKeyLength(usize),
    #[error("vault.enc 解密失败，文件可能已损坏或与 vault.key 不匹配，原文件未做任何修改")]
    DecryptFailed,
    #[error("vault.enc 格式错误（太短，不含随机数与认证标签）")]
    MalformedVault,
    #[error("加密凭证失败")]
    EncryptFailed,
    #[error("vault 文件读写失败：{0}")]
    Io(#[from] std::io::Error),
    #[error("vault 数据序列化失败：{0}")]
    Serde(#[from] serde_json::Error),
}

pub type VaultResult<T> = Result<T, VaultError>;

fn cipher(key: &[u8; KEY_LEN]) -> Aes256Gcm {
    Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key))
}

fn encrypt_bytes(key: &[u8; KEY_LEN], plaintext: &[u8]) -> VaultResult<Vec<u8>> {
    let mut nonce = [0u8; NONCE_LEN];
    OsRng.fill_bytes(&mut nonce);
    let ciphertext = cipher(key)
        .encrypt(Nonce::from_slice(&nonce), plaintext)
        .map_err(|_| VaultError::EncryptFailed)?;
    let mut out = Vec::with_capacity(NONCE_LEN + ciphertext.len());
    out.extend_from_slice(&nonce);
    out.extend_from_slice(&ciphertext);
    Ok(out)
}

fn decrypt_bytes(key: &[u8; KEY_LEN], data: &[u8]) -> VaultResult<Vec<u8>> {
    if data.len() < MIN_ENC_LEN {
        return Err(VaultError::MalformedVault);
    }
    let (nonce, ciphertext) = data.split_at(NONCE_LEN);
    cipher(key)
        .decrypt(Nonce::from_slice(nonce), ciphertext)
        .map_err(|_| VaultError::DecryptFailed)
}

/// Create the temp file for an atomic write. On Unix the file is created
/// with mode 0600 in the same open() call, so sensitive material (the vault
/// key) never exists on disk with looser permissions — no create-then-chmod
/// window. Windows keeps the default permissions.
#[cfg(unix)]
fn create_tmp_file(tmp: &Path) -> std::io::Result<std::fs::File> {
    use std::os::unix::fs::OpenOptionsExt;
    std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(tmp)
}

#[cfg(not(unix))]
fn create_tmp_file(tmp: &Path) -> std::io::Result<std::fs::File> {
    std::fs::File::create(tmp)
}

/// Write `bytes` to `path` via a temp file + atomic rename, so a crash
/// mid-write can never corrupt the previous good file.
fn atomic_write(path: &Path, bytes: &[u8]) -> VaultResult<()> {
    let tmp = path.with_extension("tmp");
    {
        let mut f = create_tmp_file(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    Ok(())
}

fn write_key_file(path: &Path, key: &[u8; KEY_LEN]) -> VaultResult<()> {
    atomic_write(path, key)
}

fn read_key_file(path: &Path) -> VaultResult<[u8; KEY_LEN]> {
    let bytes = std::fs::read(path)?;
    if bytes.len() != KEY_LEN {
        return Err(VaultError::InvalidKeyLength(bytes.len()));
    }
    let mut key = [0u8; KEY_LEN];
    key.copy_from_slice(&bytes);
    Ok(key)
}

fn generate_key() -> [u8; KEY_LEN] {
    let mut key = [0u8; KEY_LEN];
    OsRng.fill_bytes(&mut key);
    key
}

/// Encrypted credential store keyed by account id.
pub struct VaultStore {
    paths: PortablePaths,
    key: [u8; KEY_LEN],
    store: Mutex<HashMap<String, Value>>,
}

impl VaultStore {
    /// Open (or first-time initialize) the vault under `paths.data_dir`.
    ///
    /// State machine:
    /// - no key, no enc  -> generate key, create empty vault
    /// - key, no enc     -> load key, create empty vault
    /// - key + enc       -> load key, decrypt (error propagates, file untouched)
    /// - enc, no key     -> hard error, nothing is created or overwritten
    pub fn open(paths: &PortablePaths) -> VaultResult<Self> {
        paths.ensure_dirs()?;
        let key_exists = paths.vault_key_path.exists();
        let enc_exists = paths.vault_enc_path.exists();
        match (key_exists, enc_exists) {
            (false, true) => Err(VaultError::MissingKeyWithData),
            (false, false) => {
                let key = generate_key();
                // A failure here must abort: never continue with an
                // unpersisted key.
                write_key_file(&paths.vault_key_path, &key)?;
                let store = Self {
                    paths: paths.clone(),
                    key,
                    store: Mutex::new(HashMap::new()),
                };
                store.persist_locked(&HashMap::new())?;
                Ok(store)
            }
            (true, _) => {
                let key = read_key_file(&paths.vault_key_path)?;
                let map = if enc_exists {
                    let raw = std::fs::read(&paths.vault_enc_path)?;
                    let plain = decrypt_bytes(&key, &raw)?;
                    serde_json::from_slice::<HashMap<String, Value>>(&plain)
                        .map_err(|_| VaultError::DecryptFailed)?
                } else {
                    HashMap::new()
                };
                let store = Self {
                    paths: paths.clone(),
                    key,
                    store: Mutex::new(map),
                };
                if !enc_exists {
                    let snapshot = store.snapshot();
                    store.persist_locked(&snapshot)?;
                }
                Ok(store)
            }
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Value>> {
        self.store.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn snapshot(&self) -> HashMap<String, Value> {
        self.lock().clone()
    }

    fn persist_locked(&self, map: &HashMap<String, Value>) -> VaultResult<()> {
        let plain = serde_json::to_vec(map)?;
        let sealed = encrypt_bytes(&self.key, &plain)?;
        atomic_write(&self.paths.vault_enc_path, &sealed)
    }

    pub fn has(&self, account_id: &str) -> bool {
        self.lock().contains_key(account_id)
    }

    pub fn get(&self, account_id: &str) -> Option<Value> {
        self.lock().get(account_id).cloned()
    }

    /// Insert or replace the credentials for `account_id`. If persisting
    /// fails, the in-memory change is rolled back.
    pub fn set(&self, account_id: &str, credentials: Value) -> VaultResult<()> {
        let mut map = self.lock();
        let old = map.insert(account_id.to_string(), credentials);
        if let Err(e) = self.persist_locked(&map) {
            match old {
                Some(v) => {
                    map.insert(account_id.to_string(), v);
                }
                None => {
                    map.remove(account_id);
                }
            }
            return Err(e);
        }
        Ok(())
    }

    /// Remove the credentials for `account_id`, returning the previous value
    /// (so callers can restore it if a later step fails). If persisting
    /// fails, the in-memory change is rolled back.
    pub fn remove(&self, account_id: &str) -> VaultResult<Option<Value>> {
        let mut map = self.lock();
        let old = map.remove(account_id);
        if old.is_none() {
            return Ok(None);
        }
        match self.persist_locked(&map) {
            Ok(()) => Ok(old),
            Err(e) => {
                if let Some(v) = old {
                    map.insert(account_id.to_string(), v);
                }
                Err(e)
            }
        }
    }
}

/// App-facing handle: a vault that failed to initialize (corrupt file,
/// missing key) must surface a clear error to the account commands without
/// taking down the whole app or touching the original files.
pub struct VaultHandle {
    result: Result<VaultStore, String>,
}

impl VaultHandle {
    pub fn open(paths: &PortablePaths) -> Self {
        match VaultStore::open(paths) {
            Ok(store) => Self { result: Ok(store) },
            Err(e) => {
                // Error text never contains key material or credentials.
                eprintln!("[vault] initialization failed: {e}");
                Self {
                    result: Err(e.to_string()),
                }
            }
        }
    }

    pub fn store(&self) -> Result<&VaultStore, String> {
        self.result.as_ref().map_err(|e| e.clone())
    }

    pub fn init_error(&self) -> Option<&str> {
        self.result.as_ref().err().map(|e| e.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn temp_paths(tag: &str) -> PortablePaths {
        let root = std::env::temp_dir().join(format!(
            "desss-vault-test-{}-{}",
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

    fn creds() -> Value {
        serde_json::json!({"wallet_address": "0xabc", "agent_private_key": "0xsecret"})
    }

    #[test]
    fn first_init_creates_key_and_empty_vault_then_roundtrips() {
        let paths = temp_paths("init");
        let store = VaultStore::open(&paths).expect("open");
        assert!(paths.vault_key_path.exists());
        assert!(paths.vault_enc_path.exists());
        store.set("acc-1", creds()).expect("set");
        drop(store);
        let reopened = VaultStore::open(&paths).expect("reopen");
        assert_eq!(reopened.get("acc-1"), Some(creds()));
    }

    #[test]
    fn missing_key_with_existing_vault_is_an_error_and_creates_nothing() {
        let paths = temp_paths("missing-key");
        let store = VaultStore::open(&paths).expect("open");
        store.set("acc-1", creds()).expect("set");
        drop(store);
        std::fs::remove_file(&paths.vault_key_path).unwrap();
        let enc_before = std::fs::read(&paths.vault_enc_path).unwrap();
        assert!(VaultStore::open(&paths).is_err());
        assert!(!paths.vault_key_path.exists(), "must not regenerate a key");
        assert_eq!(std::fs::read(&paths.vault_enc_path).unwrap(), enc_before);
    }

    #[test]
    fn corrupted_vault_is_an_error_and_file_is_untouched() {
        let paths = temp_paths("corrupt");
        let store = VaultStore::open(&paths).expect("open");
        store.set("acc-1", creds()).expect("set");
        drop(store);
        let mut raw = std::fs::read(&paths.vault_enc_path).unwrap();
        let last = raw.len() - 1;
        raw[last] ^= 0xff;
        std::fs::write(&paths.vault_enc_path, &raw).unwrap();
        assert!(VaultStore::open(&paths).is_err());
        assert_eq!(std::fs::read(&paths.vault_enc_path).unwrap(), raw);
    }

    #[test]
    fn every_encryption_uses_a_fresh_nonce() {
        let paths = temp_paths("nonce");
        let store = VaultStore::open(&paths).expect("open");
        store.set("acc-1", creds()).expect("set");
        let first = std::fs::read(&paths.vault_enc_path).unwrap();
        store.set("acc-1", creds()).expect("set again");
        let second = std::fs::read(&paths.vault_enc_path).unwrap();
        assert_ne!(first, second, "same plaintext must seal differently");
        assert_ne!(&first[..NONCE_LEN], &second[..NONCE_LEN]);
    }

    #[test]
    fn remove_persists_across_reopen() {
        let paths = temp_paths("remove");
        let store = VaultStore::open(&paths).expect("open");
        store.set("acc-1", creds()).expect("set");
        assert_eq!(store.remove("acc-1").expect("remove"), Some(creds()));
        drop(store);
        let reopened = VaultStore::open(&paths).expect("reopen");
        assert_eq!(reopened.get("acc-1"), None);
        assert!(!reopened.has("acc-1"));
    }

    #[test]
    fn key_only_state_creates_empty_vault() {
        let paths = temp_paths("key-only");
        let store = VaultStore::open(&paths).expect("open");
        drop(store);
        std::fs::remove_file(&paths.vault_enc_path).unwrap();
        let reopened = VaultStore::open(&paths).expect("reopen with key only");
        assert_eq!(reopened.get("acc-1"), None);
        assert!(paths.vault_enc_path.exists());
    }

    #[cfg(unix)]
    #[test]
    fn key_file_has_0600_permissions_on_unix() {
        use std::os::unix::fs::PermissionsExt;
        let paths = temp_paths("perms");
        let _ = VaultStore::open(&paths).expect("open");
        let mode = std::fs::metadata(&paths.vault_key_path)
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
    }

    #[test]
    fn paths_point_into_data_dir() {
        let paths = temp_paths("layout");
        assert!(paths.vault_key_path.starts_with(&paths.data_dir));
        assert!(paths.vault_enc_path.starts_with(&paths.data_dir));
        let _ = PathBuf::new();
    }
}
