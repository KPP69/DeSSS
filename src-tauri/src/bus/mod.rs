use crate::adapters::types::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Mutex;
use tokio::sync::broadcast;
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum AppEvent {
    Tick(Tick),
    Balance(Balance),
    Position(Position),
    Book(OrderBook),
    AccountLive(AccountLive),
    OrderUpdate(Order),
    Log {
        level: String,
        source: String,
        message: String,
        ts: i64,
    },
    System {
        message: String,
    },
}
#[derive(Clone)]
pub struct EventBus {
    tx: broadcast::Sender<AppEvent>,
    latest_books: std::sync::Arc<Mutex<HashMap<String, OrderBook>>>,
}
impl EventBus {
    pub fn new(capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(capacity);
        Self {
            tx,
            latest_books: Default::default(),
        }
    }
    pub fn publish(&self, e: AppEvent) {
        if let AppEvent::Book(b) = &e {
            if let Ok(mut m) = self.latest_books.lock() {
                m.insert(format!("{}:{}", b.adapter_id, b.symbol), b.clone());
            }
        }
        let _ = self.tx.send(e);
    }
    pub fn subscribe(&self) -> broadcast::Receiver<AppEvent> {
        self.tx.subscribe()
    }
    /// One runtime log line for the in-app console drawer (level:
    /// "info" | "warn" | "error").
    pub fn log(&self, level: &str, source: &str, message: String) {
        self.publish(AppEvent::Log {
            level: level.to_string(),
            source: source.to_string(),
            message,
            ts: chrono::Utc::now().timestamp_millis(),
        });
    }
    /// Latest book per market, for the runtime snapshot after a refresh.
    pub fn snapshot_books(&self) -> Vec<OrderBook> {
        self.latest_books
            .lock()
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default()
    }
}
impl Default for EventBus {
    fn default() -> Self {
        Self::new(1024)
    }
}
