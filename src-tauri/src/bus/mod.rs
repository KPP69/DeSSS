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
/// One runtime log line (console drawer). Kept in the bus so a refresh
/// can restore the recent console via the runtime snapshot.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LogLine {
    pub level: String,
    pub source: String,
    pub message: String,
    pub ts: i64,
}
#[derive(Clone)]
pub struct EventBus {
    tx: broadcast::Sender<AppEvent>,
    latest_books: std::sync::Arc<Mutex<HashMap<String, OrderBook>>>,
    recent_logs: std::sync::Arc<Mutex<std::collections::VecDeque<LogLine>>>,
}
impl EventBus {
    pub fn new(capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(capacity);
        Self {
            tx,
            latest_books: Default::default(),
            recent_logs: Default::default(),
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
    /// "info" | "warn" | "error"). Retained (newest 300) so a refresh
    /// restores the console, and published so the drawer sees it live.
    pub fn log(&self, level: &str, source: &str, message: String) {
        let line = LogLine {
            level: level.to_string(),
            source: source.to_string(),
            message,
            ts: chrono::Utc::now().timestamp_millis(),
        };
        if let Ok(mut logs) = self.recent_logs.lock() {
            logs.push_back(line.clone());
            while logs.len() > 300 {
                logs.pop_front();
            }
        }
        self.publish(AppEvent::Log {
            level: line.level,
            source: line.source,
            message: line.message,
            ts: line.ts,
        });
    }
    /// Latest book per market, for the runtime snapshot after a refresh.
    pub fn snapshot_books(&self) -> Vec<OrderBook> {
        self.latest_books
            .lock()
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default()
    }
    /// Recent log lines, newest first, for the runtime snapshot.
    pub fn snapshot_logs(&self) -> Vec<LogLine> {
        self.recent_logs
            .lock()
            .map(|logs| logs.iter().rev().cloned().collect())
            .unwrap_or_default()
    }
}
impl Default for EventBus {
    fn default() -> Self {
        Self::new(4096)
    }
}
