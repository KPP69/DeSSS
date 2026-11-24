use super::{types::*, AdapterResult, ExchangeAdapter};
use crate::bus::{AppEvent, EventBus};
use async_trait::async_trait;
use rand::Rng;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
pub struct MockAdapter {
    id: String,
    bus: EventBus,
    running: Arc<AtomicBool>,
}
impl MockAdapter {
    pub fn new(bus: EventBus) -> Self {
        Self {
            id: "mock".into(),
            bus,
            running: Arc::new(AtomicBool::new(false)),
        }
    }
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }
    pub fn start_loops(self: Arc<Self>) {
        if self.running.swap(true, Ordering::SeqCst) {
            return;
        }
        let a = self.clone();
        tauri::async_runtime::spawn(async move {
            let mut btc = 67000f64;
            let mut eth = 3500f64;
            let mut i = tokio::time::interval(std::time::Duration::from_millis(500));
            while a.running.load(Ordering::SeqCst) {
                i.tick().await;
                let mut rng = rand::thread_rng();
                btc = (btc * (1.0 + rng.gen_range(-0.001..0.001))).max(1.0);
                eth = (eth * (1.0 + rng.gen_range(-0.0012..0.0012))).max(1.0);
                let ts = chrono::Utc::now().timestamp_millis();
                for (sym, px) in [("BTCUSDT", btc), ("ETHUSDT", eth)] {
                    a.bus.publish(AppEvent::Tick(Tick {
                        adapter_id: "mock".into(),
                        symbol: sym.into(),
                        bid: px * 0.9999,
                        ask: px * 1.0001,
                        last: px,
                        volume24h: 12345.0,
                        ts,
                    }));
                }
            }
        });
        let b = self.clone();
        tauri::async_runtime::spawn(async move {
            let mut i = tokio::time::interval(std::time::Duration::from_secs(2));
            while b.running.load(Ordering::SeqCst) {
                i.tick().await;
                let ts = chrono::Utc::now().timestamp_millis();
                for (asset, total) in [("USDT", 100000.0), ("BTC", 0.5), ("ETH", 5.0)] {
                    b.bus.publish(AppEvent::Balance(Balance {
                        adapter_id: "mock".into(),
                        account_id: "mock-account".into(),
                        asset: asset.into(),
                        free: total * 0.95,
                        locked: total * 0.05,
                        total,
                        equity_usd: if asset == "USDT" { total } else { 0.0 },
                        ts,
                    }));
                }
            }
        });
    }
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }
}
#[async_trait]
impl ExchangeAdapter for MockAdapter {
    fn id(&self) -> &str {
        &self.id
    }
    fn name(&self) -> &str {
        "Mock"
    }
    async fn connect(&self) -> AdapterResult<()> {
        Ok(())
    }
    async fn disconnect(&self) -> AdapterResult<()> {
        self.stop();
        Ok(())
    }
    async fn subscribe_ticks(&self, _s: Vec<String>) -> AdapterResult<()> {
        Ok(())
    }
    async fn unsubscribe_ticks(&self, _s: Vec<String>) -> AdapterResult<()> {
        Ok(())
    }
    async fn get_balances(&self, account_id: &str) -> AdapterResult<Vec<Balance>> {
        Ok(vec![Balance {
            adapter_id: "mock".into(),
            account_id: account_id.into(),
            asset: "USDT".into(),
            free: 95000.0,
            locked: 5000.0,
            total: 100000.0,
            equity_usd: 100000.0,
            ts: chrono::Utc::now().timestamp_millis(),
        }])
    }
    async fn get_positions(&self, _a: &str) -> AdapterResult<Vec<Position>> {
        Ok(vec![])
    }
    async fn get_open_orders(&self, _a: &str) -> AdapterResult<Vec<Order>> {
        Ok(vec![])
    }
    async fn place_order(&self, req: OrderRequest) -> AdapterResult<Order> {
        Ok(Order {
            id: uuid::Uuid::new_v4().to_string(),
            adapter_id: req.adapter_id,
            account_id: req.account_id,
            symbol: req.symbol,
            side: req.side,
            order_type: req.order_type,
            qty: req.qty,
            price: req.price,
            status: "filled".into(),
            ts: chrono::Utc::now().timestamp_millis(),
        })
    }
    async fn cancel_order(&self, _a: &str, _o: &str) -> AdapterResult<()> {
        Ok(())
    }
}
