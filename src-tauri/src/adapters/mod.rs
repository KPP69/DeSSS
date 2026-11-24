pub mod book;
pub mod hyperliquid;
pub mod lighter;
pub mod mock;
pub mod sync;
pub mod types;
use self::types::*;
use async_trait::async_trait;
pub type AdapterResult<T> = Result<T, AdapterError>;
#[derive(Debug, thiserror::Error)]
pub enum AdapterError {
    #[error("not connected")]
    NotConnected,
    #[error("{0}")]
    Other(String),
}

/// Shared HTTP client for the read-only adapters: system-native root
/// certificates (so corporate proxies / system TLS works), a standard
/// User-Agent, and a 10s timeout. reqwest reads HTTP(S)_PROXY from the
/// environment by default, so the system proxy is honoured.
pub fn new_http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .user_agent("DeSSS/0.1.0")
        .build()
        .unwrap_or_default()
}
/// Unified exchange contract. No real exchange is implemented in scaffold phase; only MockAdapter.
#[async_trait]
pub trait ExchangeAdapter: Send + Sync {
    fn id(&self) -> &str;
    fn name(&self) -> &str;
    async fn connect(&self) -> AdapterResult<()>;
    async fn disconnect(&self) -> AdapterResult<()>;
    async fn subscribe_ticks(&self, symbols: Vec<String>) -> AdapterResult<()>;
    async fn unsubscribe_ticks(&self, symbols: Vec<String>) -> AdapterResult<()>;
    async fn get_balances(&self, account_id: &str) -> AdapterResult<Vec<Balance>>;
    async fn get_positions(&self, account_id: &str) -> AdapterResult<Vec<Position>>;
    async fn get_open_orders(&self, account_id: &str) -> AdapterResult<Vec<Order>>;
    async fn place_order(&self, req: OrderRequest) -> AdapterResult<Order>;
    async fn cancel_order(&self, account_id: &str, order_id: &str) -> AdapterResult<()>;
    /// Long-running read loop (WS + REST polling). Default is a no-op; real
    /// adapters override it. The caller spawns it once after connect().
    async fn run(&self) {}
}
