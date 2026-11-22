// Placeholder: API keys will be stored encrypted in ./data
// (never OS keychain/global config in portable mode).

use std::collections::HashMap;
use std::sync::Mutex;

pub trait Vault: Send + Sync {
    fn get(&self, key: &str) -> Option<String>;
    fn set(&self, key: &str, value: String);
}

pub struct InMemoryVault {
    store: Mutex<HashMap<String, String>>,
}

impl InMemoryVault {
    pub fn new() -> Self {
        Self {
            store: Default::default(),
        }
    }
}

impl Default for InMemoryVault {
    fn default() -> Self {
        Self::new()
    }
}

impl Vault for InMemoryVault {
    fn get(&self, k: &str) -> Option<String> {
        self.store
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(k)
            .cloned()
    }

    fn set(&self, k: &str, v: String) {
        self.store
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(k.into(), v);
    }
}
