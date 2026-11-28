//! AsterDEX V3 order methods (Agent-wallet mode): POST /fapi/v3/order,
//! DELETE /fapi/v3/order, GET /fapi/v3/openOrders, /fapi/v3/order,
//! /fapi/v3/userTrades — the same EIP-712 signature as the account
//! reads (POST: form field; GET/DELETE: query). `supports_trading()`
//! stays false until a real-machine minimal order passes.
//!
//! The V3 order endpoints require a `symbol`, but the trait methods
//! only carry an id — the symbol is resolved from the order store.
//! An order the store does not know cannot be queried.
//!
//! timeInForce (V3 doc ENUM + live exchangeInfo, 2026-11-28): GTC,
//! IOC, GTX (= Post Only), HIDDEN. Only GTC / IOC / GTX are ever sent.

use serde_json::Value;

use super::AsterdexAdapter;
use crate::core::error::{VenueError, VenueResult};
use crate::core::types::{Order, OrderInfo, OrderRequest, OrderStatus, TradeInfo};
use crate::core::util::f64_from;

/// An id field that is a JSON number on the wire (`orderId`, `id`):
/// render it without quotes, and accept a string too.
fn id_str(v: &Value) -> String {
    v.as_u64()
        .map(|n| n.to_string())
        .or_else(|| v.as_i64().map(|n| n.to_string()))
        .unwrap_or_else(|| v.as_str().unwrap_or("").to_string())
}

fn order_side(s: &str) -> String {
    s.to_ascii_lowercase()
}

/// Map a V3 order `status` to the canonical status (order §二.10).
/// A resting order with fills is PartiallyFilled, not Open.
fn map_order_status(s: &str, filled: f64) -> OrderStatus {
    match (s, filled > 0.0) {
        ("NEW", true) => OrderStatus::PartiallyFilled,
        ("NEW", false) => OrderStatus::Open,
        ("PARTIALLY_FILLED", _) => OrderStatus::PartiallyFilled,
        ("FILLED", _) => OrderStatus::Filled,
        ("CANCELED", _) => OrderStatus::Canceled,
        ("REJECTED", _) => OrderStatus::Rejected,
        ("EXPIRED", _) => OrderStatus::Expired,
        _ => OrderStatus::Unresolved,
    }
}

/// The canonical order type from a V3 `type` + `timeInForce`.
fn order_type_of(v: &Value) -> (String, Option<f64>, Option<String>) {
    let stop = f64_from(&v["stopPrice"]);
    let trigger_px = if stop > 0.0 { Some(stop) } else { None };
    match v["type"].as_str().unwrap_or("") {
        "LIMIT" => match v["timeInForce"].as_str().unwrap_or("") {
            "IOC" => ("ioc".to_string(), None, None),
            "GTX" => ("post_only".to_string(), None, None),
            _ => ("limit".to_string(), None, None),
        },
        "MARKET" => ("market".to_string(), None, None),
        "STOP" | "STOP_MARKET" => ("trigger".to_string(), trigger_px, Some("sl".to_string())),
        "TAKE_PROFIT" | "TAKE_PROFIT_MARKET" => {
            ("trigger".to_string(), trigger_px, Some("tp".to_string()))
        }
        "TRAILING_STOP_MARKET" => ("trigger".to_string(), trigger_px, None),
        other => (other.to_ascii_lowercase(), trigger_px, None),
    }
}

/// Map one userTrades row to a fill (commission is a negative
/// deduction on the wire; the fee is its magnitude).
fn parse_fills(v: &Value, info: &OrderInfo) -> Vec<TradeInfo> {
    let Some(arr) = v.as_array() else {
        return Vec::new();
    };
    arr.iter()
        .map(|f| TradeInfo {
            trade_id: id_str(&f["id"]),
            exchange_id: info.exchange_id.clone(),
            order_id: id_str(&f["orderId"]),
            client_order_id: info.client_order_id.clone(),
            account_id: info.account_id.clone(),
            symbol: f["symbol"].as_str().unwrap_or("").to_string(),
            side: order_side(f["side"].as_str().unwrap_or("")),
            price: f64_from(&f["price"]),
            quantity: f64_from(&f["qty"]),
            fee: Some(f64_from(&f["commission"]).abs()),
            fee_currency: f["commissionAsset"].as_str().map(|s| s.to_string()),
            timestamp: f["time"].as_i64().unwrap_or(0),
        })
        .collect()
}

impl AsterdexAdapter {
    /// One V3 order object → the canonical record. The thin `Order`
    /// is derived from this (`Order::from`), so the two never drift.
    fn parse_order_info(&self, v: &Value) -> OrderInfo {
        let qty = f64_from(&v["origQty"]);
        let filled = f64_from(&v["executedQty"]);
        let (order_type, trigger_px, tpsl) = order_type_of(v);
        let update = v["updateTime"].as_i64().unwrap_or(0);
        let avg = f64_from(&v["avgPrice"]);
        let price = f64_from(&v["price"]);
        OrderInfo {
            order_id: id_str(&v["orderId"]),
            client_order_id: v["clientOrderId"].as_str().unwrap_or("").to_string(),
            exchange_id: self.id.clone(),
            account_id: self.account_id.clone(),
            symbol: v["symbol"].as_str().unwrap_or("").to_string(),
            side: order_side(v["side"].as_str().unwrap_or("")),
            order_type,
            status: map_order_status(v["status"].as_str().unwrap_or(""), filled),
            price: if price > 0.0 { Some(price) } else { None },
            quantity: qty,
            filled_quantity: filled,
            remaining_quantity: (qty - filled).max(0.0),
            average_fill_price: if avg > 0.0 { Some(avg) } else { None },
            trigger_px,
            tpsl,
            error: None,
            origin: String::new(),
            created_at: v["time"].as_i64().unwrap_or(update),
            updated_at: update,
        }
    }
    /// The symbol a V3 order endpoint requires, from the order store.
    fn order_symbol(&self, cloid: Option<&str>, oid: Option<u64>) -> Result<String, VenueError> {
        let found = match (cloid, oid) {
            (Some(c), _) => self.orders.get(c),
            (None, Some(o)) => self.orders.get_by_oid(&o.to_string()),
            _ => None,
        };
        let symbol = found.map(|o| o.symbol).unwrap_or_default();
        if symbol.is_empty() {
            let msg = "无法查询订单：本地无此单记录，品种未知";
            return Err(VenueError::rejected(msg));
        }
        Ok(symbol)
    }
    /// POST /fapi/v3/order (order §二.7): timeInForce only ever takes
    /// values the endpoint explicitly supports (GTC / IOC / GTX).
    /// A MARKET order sends neither price nor timeInForce (the
    /// endpoint rejects them with PARAM_NOT_REQUIRED).
    pub(crate) async fn place_order_v3(&self, req: OrderRequest) -> VenueResult<Order> {
        self.check_cool()?;
        let side = match req.side.as_str() {
            "buy" => "BUY",
            "sell" => "SELL",
            _ => {
                return Err(VenueError::rejected(format!(
                    "不支持的下单方向：{}",
                    req.side
                )))
            }
        };
        if req.quantity <= 0.0 {
            return Err(VenueError::rejected("下单数量必须大于 0".to_string()));
        }
        let cloid = req.client_order_id.clone().unwrap_or_default();
        if cloid.is_empty() {
            return Err(VenueError::rejected("下单缺少 clientOrderId".to_string()));
        }
        let (otype, tif) = match req.order_type.as_str() {
            "limit" => ("LIMIT", Some("GTC")),
            "ioc" => ("LIMIT", Some("IOC")),
            "post_only" => ("LIMIT", Some("GTX")),
            "market" => ("MARKET", None),
            "trigger" => {
                return Err(VenueError::rejected(
                    "AsterDEX 触发单需 stopPrice，当前下单入口不支持".to_string(),
                ))
            }
            other => return Err(VenueError::rejected(format!("不支持的订单类型：{other}"))),
        };
        let mut params = format!(
            "symbol={}&side={side}&type={otype}&quantity={}&newClientOrderId={}&reduceOnly={}",
            req.symbol, req.quantity, cloid, req.reduce_only,
        );
        if let Some(t) = tif {
            params.push_str(&format!("&timeInForce={t}"));
        }
        if otype != "MARKET" {
            let price = req
                .price
                .filter(|p| *p > 0.0)
                .ok_or_else(|| VenueError::rejected("限价单必须有大于 0 的价格".to_string()))?;
            params.push_str(&format!("&price={price}"));
        }
        let v = self.signed_post_v3("/fapi/v3/order", &params).await?;
        Ok(Order::from(&self.parse_order_info(&v)))
    }
    /// DELETE /fapi/v3/order (order §二.8): exactly one of
    /// origClientOrderId (`0x…` cloid) or orderId (numeric oid).
    pub(crate) async fn cancel_order_v3(&self, order_id: &str, symbol: &str) -> VenueResult<()> {
        self.check_cool()?;
        if symbol.is_empty() {
            return Err(VenueError::rejected("撤单缺少品种".to_string()));
        }
        let params = if order_id.starts_with("0x") {
            format!("symbol={symbol}&origClientOrderId={order_id}")
        } else if order_id.parse::<u64>().is_ok() {
            format!("symbol={symbol}&orderId={order_id}")
        } else {
            return Err(VenueError::rejected(format!(
                "无法识别的订单号：{order_id}"
            )));
        };
        self.signed_delete_v3("/fapi/v3/order", &params).await?;
        Ok(())
    }
    /// GET /fapi/v3/openOrders (order §二.9): no symbol, so all
    /// symbols' open orders (weight 40). A resting order with fills
    /// maps to PartiallyFilled, so the fill is visible in the status.
    pub(crate) async fn open_orders_v3(&self) -> VenueResult<Vec<Order>> {
        self.check_cool()?;
        let v = self
            .signed_order_v3(reqwest::Method::GET, "/fapi/v3/openOrders", "")
            .await?;
        let arr = v.as_array().cloned().unwrap_or_default();
        Ok(arr
            .iter()
            .map(|o| Order::from(&self.parse_order_info(o)))
            .collect())
    }
    /// GET /fapi/v3/order by cloid (order §二.10).
    pub(crate) async fn query_order_by_cloid_v3(&self, cloid: &str) -> VenueResult<OrderInfo> {
        let symbol = self.order_symbol(Some(cloid), None)?;
        self.query_order_v3(&symbol, &format!("origClientOrderId={cloid}"))
            .await
    }
    /// GET /fapi/v3/order by numeric oid (order §二.10).
    pub(crate) async fn query_order_by_oid_v3(&self, oid: u64) -> VenueResult<OrderInfo> {
        let symbol = self.order_symbol(None, Some(oid))?;
        self.query_order_v3(&symbol, &format!("orderId={oid}"))
            .await
    }
    async fn query_order_v3(&self, symbol: &str, id_params: &str) -> VenueResult<OrderInfo> {
        self.check_cool()?;
        let params = format!("symbol={symbol}&{id_params}");
        let v = self
            .signed_order_v3(reqwest::Method::GET, "/fapi/v3/order", &params)
            .await?;
        Ok(self.parse_order_info(&v))
    }
    /// GET /fapi/v3/userTrades?symbol&orderId (order §二.11): the
    /// cloid is resolved to its order first (userTrades has no cloid).
    pub(crate) async fn query_fills_v3(&self, cloid: &str) -> VenueResult<Vec<TradeInfo>> {
        let info = self.query_order_by_cloid_v3(cloid).await?;
        let oid: u64 = info
            .order_id
            .parse()
            .map_err(|_| VenueError::rejected("订单号无法解析，无法查询成交"))?;
        let params = format!("symbol={}&orderId={oid}", info.symbol);
        let v = self
            .signed_order_v3(reqwest::Method::GET, "/fapi/v3/userTrades", &params)
            .await?;
        Ok(parse_fills(&v, &info))
    }
}

#[cfg(test)]
mod tests {
    use super::super::sign::*;
    use super::*;
    use crate::bus::EventBus;
    use crate::market::reconcile::ReconcileState;
    use crate::store::orders::OrderStore;
    use std::collections::HashMap;
    use std::sync::{Arc, RwLock};

    /// A test adapter (no network): the order store holds one known
    /// order so the cloid → symbol resolution can be exercised.
    fn adapter_with_order() -> AsterdexAdapter {
        let orders = Arc::new(OrderStore::new());
        orders.insert(OrderInfo {
            order_id: "1917641".to_string(),
            client_order_id: "0xabc".to_string(),
            exchange_id: "asterdex".to_string(),
            account_id: "acc-1".to_string(),
            symbol: "BTCUSDT".to_string(),
            side: "buy".to_string(),
            order_type: "limit".to_string(),
            status: OrderStatus::Open,
            price: Some(60000.0),
            quantity: 0.4,
            filled_quantity: 0.0,
            remaining_quantity: 0.4,
            average_fill_price: None,
            trigger_px: None,
            tpsl: None,
            error: None,
            origin: "manual".to_string(),
            created_at: 1579276756075,
            updated_at: 1579276756075,
        });
        AsterdexAdapter::new_agent(
            "asterdex",
            "acc-1".to_string(),
            "0x1111111111111111111111111111111111111111".to_string(),
            "0x7f225583e3bce24e0deaa116ecff1e552cb74272".to_string(),
            [0xac; 32],
            EventBus::new(16),
            Arc::new(RwLock::new(HashMap::new())),
            orders,
            Arc::new(ReconcileState::new()),
        )
    }

    /// The V3 doc's order object shape (place / query / cancel /
    /// openOrders share it), with the order's own numbers.
    fn order_json(status: &str, executed: &str, avg: &str) -> Value {
        serde_json::json!({
            "avgPrice": avg, "clientOrderId": "0xabc", "cumQuote": "0",
            "executedQty": executed, "orderId": 1917641, "origQty": "0.40",
            "price": "60000", "reduceOnly": false, "side": "BUY",
            "positionSide": "BOTH", "status": status, "stopPrice": "0",
            "closePosition": false, "symbol": "BTCUSDT", "time": 1579276756075i64,
            "timeInForce": "GTC", "type": "LIMIT", "origType": "LIMIT",
            "updateTime": 1579276756100i64, "workingType": "CONTRACT_PRICE",
            "priceProtect": false
        })
    }

    #[test]
    fn place_response_parses_to_open_order() {
        let a = adapter_with_order();
        let info = a.parse_order_info(&order_json("NEW", "0", "0"));
        assert_eq!(info.order_id, "1917641");
        assert_eq!(info.client_order_id, "0xabc");
        assert_eq!(info.symbol, "BTCUSDT");
        assert_eq!(info.side, "buy");
        assert_eq!(info.order_type, "limit");
        assert_eq!(info.status, OrderStatus::Open);
        assert!((info.quantity - 0.4).abs() < 1e-9);
        assert_eq!(info.filled_quantity, 0.0);
        assert!((info.remaining_quantity - 0.4).abs() < 1e-9);
        assert_eq!(info.average_fill_price, None);
        assert_eq!(info.price, Some(60000.0));
        assert_eq!(info.created_at, 1579276756075);
        assert_eq!(info.updated_at, 1579276756100);
        let thin = Order::from(&info);
        assert_eq!(thin.status, OrderStatus::Open);
        assert_eq!(thin.order_id, "1917641");
    }

    #[test]
    fn query_states_map_new_partial_filled() {
        let a = adapter_with_order();
        // NEW with fills is PartiallyFilled, not Open.
        let p = a.parse_order_info(&order_json("NEW", "0.10", "60000"));
        assert_eq!(p.status, OrderStatus::PartiallyFilled);
        assert!((p.filled_quantity - 0.10).abs() < 1e-9);
        assert!((p.remaining_quantity - 0.30).abs() < 1e-9);
        assert_eq!(p.average_fill_price, Some(60000.0));
        let f = a.parse_order_info(&order_json("FILLED", "0.40", "60010"));
        assert_eq!(f.status, OrderStatus::Filled);
        assert!((f.filled_quantity - 0.40).abs() < 1e-9);
        assert_eq!(f.remaining_quantity, 0.0);
        assert_eq!(f.average_fill_price, Some(60010.0));
    }

    #[test]
    fn query_states_map_canceled_rejected_expired_unknown() {
        assert_eq!(map_order_status("CANCELED", 0.0), OrderStatus::Canceled);
        assert_eq!(map_order_status("REJECTED", 0.0), OrderStatus::Rejected);
        assert_eq!(map_order_status("EXPIRED", 0.0), OrderStatus::Expired);
        assert_eq!(
            map_order_status("PARTIALLY_FILLED", 0.1),
            OrderStatus::PartiallyFilled
        );
        assert_eq!(
            map_order_status("SOMETHING_NEW", 0.0),
            OrderStatus::Unresolved
        );
    }

    #[test]
    fn cancel_response_is_the_canceled_order() {
        let a = adapter_with_order();
        let info = a.parse_order_info(&order_json("CANCELED", "0", "0"));
        assert_eq!(info.status, OrderStatus::Canceled);
        assert_eq!(info.order_id, "1917641");
    }

    #[test]
    fn order_type_maps_tif_and_triggers() {
        let limit_gtc = serde_json::json!({"type": "LIMIT", "timeInForce": "GTC"});
        assert_eq!(order_type_of(&limit_gtc).0, "limit");
        let limit_ioc = serde_json::json!({"type": "LIMIT", "timeInForce": "IOC"});
        assert_eq!(order_type_of(&limit_ioc).0, "ioc");
        let limit_gtx = serde_json::json!({"type": "LIMIT", "timeInForce": "GTX"});
        assert_eq!(order_type_of(&limit_gtx).0, "post_only");
        let market = serde_json::json!({"type": "MARKET"});
        assert_eq!(order_type_of(&market).0, "market");
        let stop = serde_json::json!({"type": "STOP", "stopPrice": "59000"});
        let (t, px, purpose) = order_type_of(&stop);
        assert_eq!(t, "trigger");
        assert_eq!(px, Some(59000.0));
        assert_eq!(purpose.as_deref(), Some("sl"));
        let tp = serde_json::json!({"type": "TAKE_PROFIT_MARKET", "stopPrice": "65000"});
        let (t, px, purpose) = order_type_of(&tp);
        assert_eq!(t, "trigger");
        assert_eq!(px, Some(65000.0));
        assert_eq!(purpose.as_deref(), Some("tp"));
    }

    #[test]
    fn user_trades_row_maps_to_fill() {
        let a = adapter_with_order();
        let info = a.parse_order_info(&order_json("FILLED", "0.40", "60010"));
        // The V3 doc's Account Trade List example, one row.
        let v = serde_json::json!([{
            "buyer": false, "commission": "-0.07819010", "commissionAsset": "USDT",
            "id": 698759, "maker": false, "orderId": 25851813, "price": "7819.01",
            "qty": "0.002", "quoteQty": "15.63802", "realizedPnl": "-0.91539999",
            "side": "SELL", "positionSide": "SHORT", "symbol": "BTCUSDT",
            "time": 1569514978020i64
        }]);
        let fills = parse_fills(&v, &info);
        assert_eq!(fills.len(), 1);
        let f = &fills[0];
        assert_eq!(f.trade_id, "698759");
        assert_eq!(f.order_id, "25851813");
        assert_eq!(f.client_order_id, "0xabc");
        assert_eq!(f.symbol, "BTCUSDT");
        assert_eq!(f.side, "sell");
        assert!((f.price - 7819.01).abs() < 1e-9);
        assert!((f.quantity - 0.002).abs() < 1e-9);
        assert!((f.fee.unwrap() - 0.07819010).abs() < 1e-9);
        assert_eq!(f.fee_currency.as_deref(), Some("USDT"));
        assert_eq!(f.timestamp, 1569514978020);
        assert!(parse_fills(&serde_json::json!([]), &info).is_empty());
    }

    #[test]
    fn cloid_symbol_resolves_from_store_and_oid_too() {
        let a = adapter_with_order();
        assert_eq!(a.order_symbol(Some("0xabc"), None).unwrap(), "BTCUSDT");
        assert_eq!(a.order_symbol(None, Some(1917641)).unwrap(), "BTCUSDT");
        assert!(a.order_symbol(Some("0xunknown"), None).is_err());
        assert!(a.order_symbol(None, Some(999)).is_err());
    }

    #[test]
    fn order_param_string_sign_recovers_agent_address() {
        // Order §五.15: one known order param string, signed with the
        // test key — the signature must recover the agent address.
        // Same golden-vector approach as the account reads in rest.rs.
        let key = hex::decode("ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f3ff80")
            .unwrap();
        let secret: [u8; 32] = key.try_into().unwrap();
        let agent = "0x7f225583e3bce24e0deaa116ecff1e552cb74272";
        assert_eq!(address_from_secret(&secret).to_lowercase(), agent);
        let msg = "symbol=BTCUSDT&side=BUY&type=LIMIT&quantity=0.01&newClientOrderId=0xabc&reduceOnly=false&timeInForce=GTC&price=60000&user=0x1111111111111111111111111111111111111111&signer=0x7f225583e3bce24e0deaa116ecff1e552cb74272&nonce=1795588490732000";
        let sig_hex = sign_v3(&secret, msg).expect("sign");
        assert_eq!(sig_hex.len(), 130);
        let sig_bytes = hex::decode(&sig_hex).unwrap();
        let v = sig_bytes[64];
        assert!(v == 27 || v == 28);
        let sig: k256::ecdsa::Signature =
            k256::ecdsa::Signature::from_bytes((&sig_bytes[..64]).into()).expect("sig");
        let recid = k256::ecdsa::RecoveryId::from_byte(v - 27).expect("recid");
        let vk = k256::ecdsa::VerifyingKey::recover_from_prehash(&eip712_digest(msg), &sig, recid)
            .expect("recover");
        let pt = vk.to_encoded_point(false);
        let hash = keccak256(&pt.as_bytes()[1..]);
        assert_eq!(
            format!("0x{}", hex::encode(&hash[12..])).to_lowercase(),
            agent
        );
    }
}
