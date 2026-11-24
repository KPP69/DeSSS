pub mod types; pub mod mock; pub mod book; pub mod hyperliquid; pub mod lighter; pub mod sync;
use async_trait::async_trait; use self::types::*;
pub type AdapterResult<T> = Result<T, AdapterError>;
#[derive(Debug, thiserror::Error)] pub enum AdapterError { #[error("not connected")] NotConnected, #[error("{0}")] Other(String) }
/// Unified exchange contract. No real exchange is implemented in scaffold phase; only MockAdapter.
#[async_trait]
pub trait ExchangeAdapter: Send + Sync {
  fn id(&self) -> &str; fn name(&self) -> &str;
  async fn connect(&self) -> AdapterResult<()>; async fn disconnect(&self) -> AdapterResult<()>;
  async fn subscribe_ticks(&self, symbols: Vec<String>) -> AdapterResult<()>; async fn unsubscribe_ticks(&self, symbols: Vec<String>) -> AdapterResult<()>;
  async fn get_balances(&self, account_id: &str) -> AdapterResult<Vec<Balance>>;
  async fn get_positions(&self, account_id: &str) -> AdapterResult<Vec<Position>>;
  async fn get_open_orders(&self, account_id: &str) -> AdapterResult<Vec<Order>>;
  async fn place_order(&self, req: OrderRequest) -> AdapterResult<Order>;
  async fn cancel_order(&self, account_id: &str, order_id: &str) -> AdapterResult<()>;
  /// Long-running read loop (WS + REST polling). Default is a no-op; real
  /// adapters override it. The caller spawns it once after connect().
  async fn run(&self) {}
}
