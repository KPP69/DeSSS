//! Hyperliquid / Entropy orders: placement, cancel, status queries
//! and fills, plus the `ExchangeAdapter` surface. Moved verbatim from
//! `adapters/hyperliquid.rs` (PR-1.2).

use std::sync::atomic::Ordering;
use std::time::Instant;

use async_trait::async_trait;
use serde_json::Value;

use super::sign::{
    cancel_action_json, cancel_action_mp, cancel_by_cloid_action_json, cancel_by_cloid_action_mp,
};
use super::{AssetMeta, HyperliquidAdapter, ASSET_META_TTL};
use crate::core::error::{VenueError, VenueResult};
use crate::core::signing::{self, Mp, NonceAllocator};
use crate::core::types::{
    Balance, Order, OrderInfo, OrderRequest, OrderStatus, Position, TradeInfo,
};
use crate::core::util::{f64_from, now_ms};
use crate::venues::ExchangeAdapter;

/// A wire price/size string: Decimal, at most 8 decimals (the SDK's
/// `float_to_wire` rule), trailing zeros stripped.
fn decimal_to_wire(v: f64) -> Result<String, String> {
    // `to_decimal` maps NaN / infinity to zero; the caller checks
    // > 0, so an invalid number can never reach the wire.
    let d = crate::core::precision::to_decimal(v);
    if d.scale() > 8 {
        return Err("价格/数量精度超过 8 位小数".to_string());
    }
    Ok(d.normalize().to_string())
}

/// `openOrders` / `frontendOpenOrders` side: "A" is the ask (sell),
/// "B" is the bid (buy).
fn hl_side_to_side(s: &str) -> String {
    if s == "A" {
        "sell".to_string()
    } else {
        "buy".to_string()
    }
}

/// Map an `orderStatus` status string onto the canonical enum.
/// Anything unrecognised stays Unresolved — a status we do not
/// understand is never guessed into Filled or Canceled.
fn map_order_status(s: &str) -> OrderStatus {
    match s {
        "open" | "triggered" => OrderStatus::Open,
        "filled" => OrderStatus::Filled,
        s if s == "canceled" || s.ends_with("Canceled") => OrderStatus::Canceled,
        s if s == "rejected" || s.ends_with("Rejected") => OrderStatus::Rejected,
        _ => OrderStatus::Unresolved,
    }
}

/// Parse one `orderStatus` response into an OrderInfo. `fills_avg`
/// is the volume-weighted average fill price from the fills query
/// (None when the order never filled or the fills query failed — the
/// caller falls back to the limit price and logs it).
/// TP/SL purpose from the exchange's human-readable order type
/// ("Take Profit Market" / "Stop Loss Market"); None when the type
/// does not say — plain `openOrders` reports "Trigger Market",
/// which carries no tp/sl, so the caller derives it from the
/// position or keeps the placement intent's value (see
/// `apply_query`).
fn tpsl_from_order_type(order_type: &str) -> Option<String> {
    if order_type.contains("Take Profit") {
        Some("tp".to_string())
    } else if order_type.contains("Stop") {
        Some("sl".to_string())
    } else {
        None
    }
}

fn parse_order_status_response(
    v: &Value,
    exchange_id: &str,
    account_id: &str,
    cloid: &str,
    fills_avg: Option<f64>,
) -> Result<OrderInfo, VenueError> {
    if v["status"].as_str() == Some(crate::core::error::UNKNOWN_OID) {
        return Err(VenueError::rejected(
            crate::core::error::UNKNOWN_OID.to_string(),
        ));
    }
    let inner = &v["order"];
    let o = &inner["order"];
    if o.is_null() {
        return Err(VenueError::transport("订单状态响应无法解析".to_string()));
    }
    let status = map_order_status(inner["status"].as_str().unwrap_or(""));
    let orig_sz = f64_from(&o["origSz"]);
    let remaining = f64_from(&o["sz"]);
    let filled = (orig_sz - remaining).max(0.0);
    let status = if status == OrderStatus::Open && filled > 0.0 {
        OrderStatus::PartiallyFilled
    } else {
        status
    };
    let limit_px = f64_from(&o["limitPx"]);
    let avg = if filled > 0.0 {
        fills_avg.or(Some(limit_px))
    } else {
        None
    };
    let tif = o["tif"].as_str().unwrap_or("Gtc");
    // A trigger order's TIF is meaningless (it fires at market);
    // record it as "trigger" so the type tells the truth.
    let is_trigger = o["isTrigger"].as_bool().unwrap_or(false);
    let order_type = if is_trigger {
        "trigger".to_string()
    } else {
        match tif {
            "Alo" => "post_only",
            "Ioc" => "ioc",
            _ => "limit",
        }
        .to_string()
    };
    Ok(OrderInfo {
        order_id: o["oid"].as_u64().map(|n| n.to_string()).unwrap_or_default(),
        client_order_id: o["cloid"]
            .as_str()
            .map(|c| c.to_string())
            .filter(|c| !c.is_empty())
            .unwrap_or_else(|| cloid.to_string()),
        exchange_id: exchange_id.to_string(),
        account_id: account_id.to_string(),
        symbol: o["coin"].as_str().unwrap_or("").to_string(),
        side: hl_side_to_side(o["side"].as_str().unwrap_or("B")),
        order_type,
        status,
        price: Some(limit_px),
        quantity: orig_sz,
        filled_quantity: filled,
        remaining_quantity: remaining,
        average_fill_price: avg,
        error: None,
        trigger_px: if is_trigger {
            Some(f64_from(&o["triggerPx"])).filter(|p| *p > 0.0)
        } else {
            None
        },
        tpsl: if is_trigger {
            tpsl_from_order_type(o["orderType"].as_str().unwrap_or(""))
        } else {
            None
        },
        // A queried record carries no origin; the caller keeps the
        // stored one (see `apply_query`). "manual" is only the
        // placeholder for a record the store has never seen.
        origin: "manual".to_string(),
        created_at: o["timestamp"].as_i64().unwrap_or(0),
        updated_at: inner["statusTimestamp"].as_i64().unwrap_or(0),
    })
}

/// Parse an `openOrders` response into wire orders.
fn parse_open_orders(v: &Value, exchange_id: &str, account_id: &str) -> Result<Vec<Order>, String> {
    let arr = v
        .as_array()
        .ok_or_else(|| "挂单列表响应无法解析".to_string())?;
    Ok(arr
        .iter()
        .filter_map(|o| {
            let oid = o["oid"].as_u64()?;
            let is_trigger = o["isTrigger"].as_bool().unwrap_or(false);
            Some(Order {
                order_id: oid.to_string(),
                // The exchange returns the cloid when the order was
                // placed with one (null otherwise). It must be kept:
                // without it a website / restarted-session order can
                // only be cancelled by oid, and its post-cancel
                // reconcile has no cloid to query by.
                client_order_id: o["cloid"].as_str().unwrap_or("").to_string(),
                exchange_id: exchange_id.to_string(),
                account_id: account_id.to_string(),
                symbol: o["coin"].as_str().unwrap_or("").to_string(),
                side: hl_side_to_side(o["side"].as_str().unwrap_or("B")),
                order_type: if is_trigger {
                    "trigger".to_string()
                } else {
                    "limit".to_string()
                },
                quantity: f64_from(&o["sz"]),
                price: Some(f64_from(&o["limitPx"])),
                status: OrderStatus::Open,
                ts: o["timestamp"].as_i64().unwrap_or(0),
                trigger_px: if is_trigger {
                    Some(f64_from(&o["triggerPx"])).filter(|p| *p > 0.0)
                } else {
                    None
                },
                tpsl: if is_trigger {
                    tpsl_from_order_type(o["orderType"].as_str().unwrap_or(""))
                } else {
                    None
                },
            })
        })
        .collect())
}

/// Parse a `userFillsByTime` response into TradeInfo rows for one
/// oid. The exchange returns the fills newest-first; order here is
/// kept as returned (the store sorts by timestamp on display).
fn parse_fills(
    v: &Value,
    exchange_id: &str,
    account_id: &str,
    cloid: &str,
    oid: u64,
) -> Vec<TradeInfo> {
    let Some(arr) = v.as_array() else {
        return Vec::new();
    };
    arr.iter()
        .filter(|f| f["oid"].as_u64() == Some(oid))
        .map(|f| TradeInfo {
            trade_id: f["tid"].as_u64().map(|n| n.to_string()).unwrap_or_default(),
            exchange_id: exchange_id.to_string(),
            order_id: oid.to_string(),
            client_order_id: cloid.to_string(),
            account_id: account_id.to_string(),
            symbol: f["coin"].as_str().unwrap_or("").to_string(),
            side: hl_side_to_side(f["side"].as_str().unwrap_or("B")),
            price: f64_from(&f["px"]),
            quantity: f64_from(&f["sz"]),
            fee: Some(f64_from(&f["fee"])),
            fee_currency: Some(f["feeToken"].as_str().unwrap_or("USDC").trim().to_string()),
            timestamp: f["time"].as_i64().unwrap_or(0),
        })
        .collect()
}

/// HIP-3 dex offset from the null-filtered `perpDexs` name list:
/// the official SDK's rule (`hyperliquid/info.py`,
/// `for i, perp_dex in enumerate(self.perp_dexs()[1:])` →
/// `110000 + i * 10000`). `names` is that filtered list — the raw
/// response's `[0]` is the null main slot, so the 0-based index here
/// IS the SDK's `i`. (The old code subtracted 1 again and put `io`
/// at 190000 instead of 200000.)
fn dex_offset_for(names: &[String], dex: &str) -> Option<u32> {
    names
        .iter()
        .position(|n| n == dex)
        .map(|pos| 110_000 + pos as u32 * 10_000)
}

/// The wire "t" value: a limit order carries its TIF; a trigger
/// order carries isMarket / triggerPx / tpsl (HL docs: the "t" object
/// is either {"limit": {"tif": ...}} or {"trigger": {...}}).
pub(crate) enum OrderTypeWire<'a> {
    Limit(&'a str),
    Trigger {
        is_market: bool,
        trigger_px: &'a str,
        tpsl: &'a str,
    },
}

/// Build the signed order action — msgpack form (for the signature)
/// and JSON form (for the POST body) — from the already-wired
/// strings. Key order follows the official SDK
/// (`order_request_to_order_wire` / `order_wires_to_order_action`).
///
/// D8: `p` is the worst-acceptable (limit) price even for a market
/// trigger (isMarket=true) — the official SDK example
/// `basic_tpsl.py` sets the stop's limit ~3-4% beyond the trigger
/// and the take-profit's limit equal to it, so a stop whose `p`
/// equals its trigger can rest unfilled through a gap. The caller
/// (execution::place_order) pre-buffers a stop-loss `p` by
/// SL_SLIPPAGE_BPS; a take-profit's `p` is its trigger price. This
/// builder only wires the strings it is given.
/// a, b, p, s, r, t, c inside each order; type, orders, grouping
/// outside. Grouping follows the variant: a trigger order is a
/// position TP/SL ("positionTpsl"), a plain order is ungrouped ("na").
/// The golden-vector test in `core::signing` covers this builder
/// against the SDK's own bytes, so a key-order slip here fails loudly.
pub(crate) fn build_order_action<'a>(
    asset: u32,
    is_buy: bool,
    price: &'a str,
    qty: &'a str,
    reduce_only: bool,
    order_type: OrderTypeWire<'a>,
    cloid: &'a str,
) -> (Mp<'a>, serde_json::Value) {
    let (t_mp, t_json, grouping) = match order_type {
        OrderTypeWire::Limit(tif) => (
            Mp::Map(vec![("limit", Mp::Map(vec![("tif", Mp::Str(tif))]))]),
            serde_json::json!({"limit": {"tif": tif}}),
            "na",
        ),
        OrderTypeWire::Trigger {
            is_market,
            trigger_px,
            tpsl,
        } => (
            Mp::Map(vec![(
                "trigger",
                Mp::Map(vec![
                    ("isMarket", Mp::Bool(is_market)),
                    ("triggerPx", Mp::Str(trigger_px)),
                    ("tpsl", Mp::Str(tpsl)),
                ]),
            )]),
            serde_json::json!({"trigger": {"isMarket": is_market, "triggerPx": trigger_px, "tpsl": tpsl}}),
            "positionTpsl",
        ),
    };
    let action_mp = Mp::Map(vec![
        ("type", Mp::Str("order")),
        (
            "orders",
            Mp::Array(vec![Mp::Map(vec![
                ("a", Mp::UInt(asset as u64)),
                ("b", Mp::Bool(is_buy)),
                ("p", Mp::Str(price)),
                ("s", Mp::Str(qty)),
                ("r", Mp::Bool(reduce_only)),
                ("t", t_mp),
                ("c", Mp::Str(cloid)),
            ])]),
        ),
        ("grouping", Mp::Str(grouping)),
    ]);
    let action_json = serde_json::json!({
        "type": "order",
        "orders": [{
            "a": asset,
            "b": is_buy,
            "p": price,
            "s": qty,
            "r": reduce_only,
            "t": t_json,
            "c": cloid,
        }],
        "grouping": grouping,
    });
    (action_mp, action_json)
}

impl HyperliquidAdapter {
    /// Resolve a catalog symbol to its asset index, using the cached
    /// meta universe (5 min TTL). The symbol is used verbatim
    /// (`io:NVDA` for Entropy — the `io` universe names already
    /// carry the prefix); the asset is `dex_offset + index in that
    /// dex's universe`, with the offset from [`dex_offset_for`].
    async fn resolve_asset(&self, symbol: &str) -> Result<u32, VenueError> {
        {
            let cache = self.asset_meta.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(m) = cache.as_ref() {
                if m.fetched_at.elapsed() < ASSET_META_TTL {
                    return m
                        .universe
                        .iter()
                        .position(|n| n == symbol)
                        .map(|i| m.dex_offset + i as u32)
                        .ok_or_else(|| {
                            VenueError::rejected(format!("{symbol} 不在该交易所的交易品种中"))
                        });
                }
            }
        }
        let dex = self.dex_name();
        let mut meta_body = serde_json::json!({"type": "meta"});
        if !dex.is_empty() {
            meta_body["dex"] = Value::String(dex.to_string());
        }
        let meta = self.post_info(meta_body).await?;
        let universe: Vec<String> = meta["universe"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|u| u["name"].as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        if universe.is_empty() {
            return Err(VenueError::rejected(
                "Hyperliquid 品种列表为空，无法解析交易对".to_string(),
            ));
        }
        let mut dex_offset = 0u32;
        if !dex.is_empty() {
            let dexs = self
                .post_info(serde_json::json!({"type": "perpDexs"}))
                .await?;
            let names: Vec<String> = dexs
                .as_array()
                .map(|arr| {
                    arr.iter()
                        .filter_map(|d| d["name"].as_str().map(|s| s.to_string()))
                        .collect()
                })
                .unwrap_or_default();
            dex_offset = dex_offset_for(&names, dex).ok_or_else(|| {
                VenueError::rejected(format!("交易所未返回 {dex} dex，无法解析交易对"))
            })?;
        }
        let asset = universe
            .iter()
            .position(|n| n == symbol)
            .map(|i| dex_offset + i as u32)
            .ok_or_else(|| VenueError::rejected(format!("{symbol} 不在该交易所的交易品种中")))?;
        *self.asset_meta.lock().unwrap_or_else(|e| e.into_inner()) = Some(AssetMeta {
            fetched_at: Instant::now(),
            universe,
            dex_offset,
        });
        Ok(asset)
    }

    /// One `orderStatus` lookup. `numeric` posts the id as a JSON
    /// number (a real oid); otherwise it is posted as a string (a
    /// cloid). The two forms must never be confused: the exchange
    /// rejects a decimal oid sent as a string (verified live
    /// 2026-11-26), and the parse below needs the id back as the
    /// fallback `client_order_id` for orders the exchange reports
    /// without one.
    async fn query_order(&self, id: &str, numeric: bool) -> VenueResult<OrderInfo> {
        let oid_json = if numeric {
            let n: u64 = id
                .parse()
                .map_err(|_| VenueError::rejected("订单号无法解析".to_string()))?;
            serde_json::json!(n)
        } else {
            Value::String(id.to_string())
        };
        let v = self
            .post_info(serde_json::json!({
                "type": "orderStatus",
                "user": self.address,
                "oid": oid_json,
            }))
            .await?;
        // Fill details need the oid + order time; fetch them first so
        // the average price below is real, not the limit price.
        let mut fills_avg = None;
        if let Some(o) = v["order"]["order"].as_object() {
            let oid = o["oid"].as_u64().unwrap_or(0);
            let ts = o["timestamp"].as_i64().unwrap_or(0);
            if oid != 0 && ts > 0 {
                fills_avg = self.fills_avg(oid, ts).await;
            }
        }
        parse_order_status_response(&v, &self.id, &self.account_id, id, fills_avg)
    }

    /// Volume-weighted average fill price of one order, from
    /// `userFillsByTime` (matched by oid). `None` when there are no
    /// fills yet or the query fails — the caller falls back to the
    /// limit price and logs it.
    async fn fills_avg(&self, oid: u64, created_at_ms: i64) -> Option<f64> {
        let v = self
            .post_info(serde_json::json!({
                "type": "userFillsByTime",
                "user": self.address,
                "startTime": created_at_ms,
                "aggregateByTime": true,
            }))
            .await
            .ok()?;
        let arr = v.as_array()?;
        let mut qty = 0.0f64;
        let mut notional = 0.0f64;
        for f in arr {
            if f["oid"].as_u64() != Some(oid) {
                continue;
            }
            let sz = f64_from(&f["sz"]);
            let px = f64_from(&f["px"]);
            qty += sz;
            notional += sz * px;
        }
        if qty > 0.0 {
            Some(notional / qty)
        } else {
            None
        }
    }
}

#[async_trait]
impl ExchangeAdapter for HyperliquidAdapter {
    fn id(&self) -> &str {
        &self.id
    }
    fn name(&self) -> &str {
        if self.dex.is_some() {
            "Entropy"
        } else {
            "Hyperliquid"
        }
    }

    async fn connect(&self) -> VenueResult<()> {
        self.running.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn disconnect(&self) -> VenueResult<()> {
        self.running.store(false, Ordering::SeqCst);
        Ok(())
    }

    async fn subscribe_ticks(&self, _symbols: Vec<String>) -> VenueResult<()> {
        Ok(())
    }
    async fn unsubscribe_ticks(&self, _symbols: Vec<String>) -> VenueResult<()> {
        Ok(())
    }

    async fn get_balances(&self, _account_id: &str) -> VenueResult<Vec<Balance>> {
        let (_, balance) = self
            .fetch_account_state()
            .await
            .map_err(VenueError::transport)?;
        Ok(vec![balance])
    }

    async fn get_positions(&self, _account_id: &str) -> VenueResult<Vec<Position>> {
        let (snap, _) = self
            .fetch_account_state()
            .await
            .map_err(VenueError::transport)?;
        Ok(snap
            .positions
            .iter()
            .map(|p| Position {
                adapter_id: self.id.clone(),
                account_id: self.account_id.clone(),
                symbol: p.symbol.clone(),
                side: p.side.clone(),
                qty: p.qty,
                entry_price: p.entry_price,
                mark_price: p.mark_price,
                unrealized_pnl: p.unrealized_pnl,
            })
            .collect())
    }

    async fn get_open_orders(&self, _account_id: &str) -> VenueResult<Vec<Order>> {
        let mut body = serde_json::json!({
            "type": "openOrders",
            "user": self.address,
        });
        let dex = self.dex_name();
        if !dex.is_empty() {
            body["dex"] = Value::String(dex.to_string());
        }
        let v = self.post_info(body).await?;
        parse_open_orders(&v, &self.id, &self.account_id).map_err(VenueError::transport)
    }

    async fn place_order(&self, req: OrderRequest) -> VenueResult<Order> {
        let (_secret, agent_addr) = self.signing_key()?;
        let asset = self.resolve_asset(&req.symbol).await?;
        let price = req
            .price
            .filter(|p| *p > 0.0)
            .ok_or_else(|| VenueError::rejected("限价单必须有大于 0 的价格".to_string()))?;
        if req.quantity <= 0.0 {
            return Err(VenueError::rejected("下单数量必须大于 0".to_string()));
        }
        let price_wire = decimal_to_wire(price).map_err(VenueError::rejected)?;
        let qty_wire = decimal_to_wire(req.quantity).map_err(VenueError::rejected)?;
        let cloid = req
            .client_order_id
            .clone()
            .filter(|c| !c.is_empty())
            .unwrap_or_else(signing::generate_cloid);
        // A trigger price turns this into a trigger (TP/SL) order:
        // the wire "t" is the trigger variant and grouping is
        // "positionTpsl" (tied to the position, not to an entry
        // order). "p" stays the reference / worst-acceptable price —
        // the caller sends it equal to the trigger price. Without a
        // trigger price this is a plain limit order with its TIF.
        // Declared without an initial value: only the trigger arm
        // assigns it, and only there is it borrowed (the enum carries
        // the borrow into build_order_action below). No dummy empty
        // string, no assignment that is never read.
        let trigger_px_wire: String;
        let order_type = if let Some(tp) = req.trigger_px {
            if !(tp > 0.0) {
                return Err(VenueError::rejected("触发价必须大于 0".to_string()));
            }
            let tpsl = match req.tpsl.as_deref() {
                Some("tp") | Some("sl") => req.tpsl.as_deref().unwrap(),
                _ => {
                    return Err(VenueError::rejected(
                        "触发单必须指定 tpsl 为 tp 或 sl".to_string(),
                    ))
                }
            };
            trigger_px_wire = decimal_to_wire(tp).map_err(VenueError::rejected)?;
            OrderTypeWire::Trigger {
                is_market: req.is_market.unwrap_or(true),
                trigger_px: &trigger_px_wire,
                tpsl,
            }
        } else {
            let tif = match req.order_type.as_str() {
                "limit" => "Gtc",
                "post_only" => "Alo",
                "ioc" => "Ioc",
                other => {
                    return Err(VenueError::rejected(format!(
                        "不支持的订单类型：{other}（仅支持 limit / post_only / ioc）"
                    )))
                }
            };
            OrderTypeWire::Limit(tif)
        };
        let nonce = NonceAllocator::next(&agent_addr);
        let (action_mp, action_json) = build_order_action(
            asset,
            req.side == "buy",
            &price_wire,
            &qty_wire,
            req.reduce_only,
            order_type,
            &cloid,
        );
        let v = self.post_exchange(action_json, &action_mp, nonce).await?;
        if v["status"].as_str() == Some("err") {
            return Err(VenueError::rejected(format!(
                "交易所拒绝下单：{}",
                v["response"].as_str().unwrap_or("未知原因")
            )));
        }
        let statuses = v["response"]["data"]["statuses"]
            .as_array()
            .ok_or_else(|| {
                VenueError::UnknownOutcome("下单响应中没有订单状态，订单状态未知".to_string())
            })?;
        let first = statuses.first().ok_or_else(|| {
            VenueError::UnknownOutcome("下单响应中没有订单状态，订单状态未知".to_string())
        })?;
        let (oid, status) = if let Some(resting) = first.get("resting") {
            (
                resting["oid"].as_u64().unwrap_or(0).to_string(),
                OrderStatus::Open,
            )
        } else if let Some(filled) = first.get("filled") {
            (
                filled["oid"].as_u64().unwrap_or(0).to_string(),
                OrderStatus::Filled,
            )
        } else if let Some(err) = first.get("error").and_then(|e| e.as_str()) {
            return Err(VenueError::rejected(format!("交易所拒绝下单：{err}")));
        } else {
            return Err(VenueError::UnknownOutcome(
                "下单响应状态无法识别，订单状态未知".to_string(),
            ));
        };
        Ok(Order {
            order_id: oid,
            client_order_id: cloid,
            exchange_id: self.id.clone(),
            account_id: req.account_id,
            symbol: req.symbol,
            side: req.side,
            order_type: req.order_type,
            quantity: req.quantity,
            price: Some(price),
            status,
            ts: now_ms(),
            trigger_px: req.trigger_px,
            tpsl: req.tpsl,
        })
    }

    async fn cancel_order(
        &self,
        _account_id: &str,
        order_id: &str,
        symbol: &str,
    ) -> VenueResult<()> {
        let (_secret, agent_addr) = self.signing_key()?;
        let asset = self.resolve_asset(symbol).await?;
        let (action_mp, action_json) = if order_id.starts_with("0x") {
            (
                cancel_by_cloid_action_mp(asset, order_id),
                cancel_by_cloid_action_json(asset, order_id),
            )
        } else {
            let oid: u64 = order_id
                .parse()
                .map_err(|_| VenueError::rejected(format!("订单号格式不正确：{order_id}")))?;
            (cancel_action_mp(asset, oid), cancel_action_json(asset, oid))
        };
        let nonce = NonceAllocator::next(&agent_addr);
        let v = self.post_exchange(action_json, &action_mp, nonce).await?;
        if v["status"].as_str() == Some("err") {
            return Err(VenueError::rejected(format!(
                "交易所拒绝撤单：{}",
                v["response"].as_str().unwrap_or("未知原因")
            )));
        }
        let statuses = v["response"]["data"]["statuses"]
            .as_array()
            .ok_or_else(|| {
                VenueError::UnknownOutcome("撤单响应中没有撤单状态，撤单结果未知".to_string())
            })?;
        let first = statuses.first().ok_or_else(|| {
            VenueError::UnknownOutcome("撤单响应中没有撤单状态，撤单结果未知".to_string())
        })?;
        if first.as_str() == Some("success") {
            Ok(())
        } else if let Some(err) = first.get("error").and_then(|e| e.as_str()) {
            // "Order was never placed, already canceled, or filled."
            // is a definite answer about the cancel, but NOT about the
            // order: the caller queries the order to find out which.
            Err(VenueError::rejected(format!("交易所拒绝撤单：{err}")))
        } else {
            Err(VenueError::UnknownOutcome(
                "撤单响应状态无法识别，撤单结果未知".to_string(),
            ))
        }
    }

    async fn query_order_by_cloid(&self, _account_id: &str, cloid: &str) -> VenueResult<OrderInfo> {
        // A decimal oid passed as a string is NOT a cloid: the
        // exchange rejects it with "Failed to deserialize the JSON
        // body into the target type" (verified live 2026-11-26), so
        // the post-cancel reconcile of an oid-only order silently
        // kept the old state. Route it to the numeric form instead.
        if !cloid.starts_with("0x") {
            if let Ok(oid) = cloid.parse::<u64>() {
                return self.query_order_by_oid(_account_id, oid).await;
            }
        }
        self.query_order(cloid, false).await
    }

    async fn query_order_by_oid(&self, _account_id: &str, oid: u64) -> VenueResult<OrderInfo> {
        self.query_order(&oid.to_string(), true).await
    }

    async fn query_fills(&self, _account_id: &str, cloid: &str) -> VenueResult<Vec<TradeInfo>> {
        let info = self.query_order_by_cloid(_account_id, cloid).await?;
        let oid: u64 = info
            .order_id
            .parse()
            .map_err(|_| VenueError::rejected("订单号无法解析，无法查询成交".to_string()))?;
        let v = self
            .post_info(serde_json::json!({
                "type": "userFillsByTime",
                "user": self.address,
                "startTime": info.created_at,
                "aggregateByTime": true,
            }))
            .await?;
        Ok(parse_fills(&v, &self.id, &self.account_id, cloid, oid))
    }

    async fn run(&self) {
        // Account sync is REST polling only: no market WS is opened for
        // an account (public market data comes from the on-demand
        // catalog, never from a per-account connection).
        self.rest_loop().await;
    }

    fn wake_sync(&self) {
        self.wake.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// F2 evidence (2026-11-26, real `perpDexs` fetches — the full raw
    /// responses are archived under the goal workspace
    /// `evidence/perpdexs-*-2026-11-26.json`; the fixtures keep the
    /// name sequence, the only field the offset uses, in fetched
    /// order, null included): the SDK enumerates `perpDexs()[1:]`,
    /// so `io` — 10th raw, 9th after the null main slot — sits at
    /// 200000. The old formula subtracted 1 again and put it at
    /// 190000; on testnet (288 dexes) every dex past the first was
    /// 10000 short.
    const PERP_DEXS_MAINNET_JSON: &str =
        r#"[null,"xyz","flx","vntl","hyna","km","abcd","cash","para","mkts","io"]"#;
    const PERP_DEXS_TESTNET_JSON: &str = r#"[null,"test","unit","scam","felix","volmex","angry","TOON","VOLMEX","test21","pluto","zigg","i<3fl","KNETIQ",
"knetiq","ddot","venone","tstdex","vntls","oracle","hb","sports","aura","hybet","orange","sekai","flow","x","t",
"sekaw","gpu","qwerty","merrli","dex","olvb","nq","fx","nqz","btcx","woof","shiok","z","hbtwo","tndex","rrrrr",
"hbhb","stocks","nunchi","sqtwr","y","hypodd","strat","zdex","crayon","walao","slpy","tsts","mbx","food","meng",
"rawr","mmmg","msdx","wrld","hodd","xyz","flx","nmx","pyth","msda","ho","flsh","hyfl","csan","sedx","rbbt",
"rbte","demo","flxn","bsx","loh","xvm","dydx","fzx","abdc","fam","gato","bart","sss","owal","gg","fdnk","trve",
"odd","trv","qwrn","zzzz","gass","whtm","rip","sand","hmax","slob","hyfi","str","trov","erez","paef","mcd",
"birb","plsh","cnst","trvv","trvo","tvr","torv","tvro","tvor","hyna","tdex","neo","bhpx","yldx","dpro","wa",
"org","hfx","fxx","pmt","trva","trvb","perp","elxl","ddd","trvc","brd","axx","ddex","ab","etft","llll","nuts",
"jdo","otcm","xmas","rub","avtr","wgts","vrse","gst","otc","bee","trvd","unc","dddd","dxn","egrl","nyc","bmx",
"purr","vdex","narr","abcd","mox","angl","trvf","hnb","ggwp","plat","orma","ormb","ormc","ormd","orme","ormf",
"ormg","ormh","tngs","ormi","brnt","yex","ez","rbit","ysh","moo","guac","osrs","moom","idx","scpe","mag","frce",
"hodx","yoyo","cmdt","cmda","xoxo","idxx","cmdb","ycl","ohtn","zktx","soci","otni","nova","ani","soya","brs",
"tenx","jack","myx","pmas","dude","rsge","hov","dlx","dxx","dyel","blob","mooo","rolo","sejp","sjhu","dolo",
"sqxl","krzh","kwo","name","aha","tsst","neol","flxx","dd","vt","hfl","hkx","pew","magm","eepy","magk","ktob",
"nxlb","dur","shm","mmmm","acmt","cna","bb","dmhs","lah","lob","vkjf","ignp","kjex","arg","cfbx","lon","par",
"aix","parx","rrup","fcmt","nimb","sta","stt","pmew","tape","goy","eqde","one","pa","pas","lab","oxob","oxoc",
"ppp","bit","mew","vmsj","kz","qosm","poep","qhte","vmse","prb","prs","otrk","qaz","div"]"#;

    /// Parse a `perpDexs` fixture the way `resolve_asset` does:
    /// drop the null slots, keep the names in order. Fixture entries
    /// are bare names; real responses are objects with a `name`
    /// field — both shapes are accepted.
    fn perp_dex_names(json: &str) -> Vec<String> {
        serde_json::from_str::<serde_json::Value>(json)
            .expect("fixture")
            .as_array()
            .expect("array")
            .iter()
            .filter_map(|d| {
                d.as_str()
                    .or_else(|| d.get("name").and_then(|n| n.as_str()))
                    .map(|s| s.to_string())
            })
            .collect()
    }

    #[test]
    fn decimal_to_wire_normalizes_like_the_sdk() {
        // The golden vector's strings ("90000", not "90000.0") must
        // be the ones this encoder emits, or the vector proves
        // nothing about the production path.
        assert_eq!(decimal_to_wire(90000.0).unwrap(), "90000");
        assert_eq!(decimal_to_wire(0.01).unwrap(), "0.01");
        assert_eq!(decimal_to_wire(90000.5).unwrap(), "90000.5");
    }

    #[test]
    fn open_orders_parse_trigger_orders() {
        // A position TP/SL resting on the book: the frontend shows
        // it as the position's protection and the protect flow
        // cancels it by oid. The second row is what plain
        // openOrders reports for a trigger ("Trigger Market" carries
        // no tp/sl) — tpsl stays None and the frontend derives it.
        let v = serde_json::json!([
            {
                "coin": "BTC", "side": "A", "limitPx": "91000.0",
                "sz": "0.01", "oid": 7, "timestamp": 1724361546645i64,
                "triggerCondition": "Mark price >= 91000.0",
                "isTrigger": true, "triggerPx": "91000.0",
                "children": [], "isPositionTpsl": true,
                "reduceOnly": true, "orderType": "Take Profit Market",
                "cloid": "0x1234567890abcdef1234567890abcdef"
            },
            {
                "coin": "BTC", "side": "A", "limitPx": "89000.0",
                "sz": "0.01", "oid": 8, "timestamp": 1724361546645i64,
                "triggerCondition": "Mark price <= 89000.0",
                "isTrigger": true, "triggerPx": "89000.0",
                "children": [], "isPositionTpsl": true,
                "reduceOnly": true, "orderType": "Trigger Market",
                "cloid": null
            }
        ]);
        let orders = parse_open_orders(&v, "hyperliquid", "acc-1").unwrap();
        assert_eq!(orders.len(), 2);
        assert_eq!(orders[0].order_type, "trigger");
        assert_eq!(orders[0].trigger_px, Some(91000.0));
        assert_eq!(orders[0].tpsl.as_deref(), Some("tp"));
        assert_eq!(orders[1].order_type, "trigger");
        assert_eq!(orders[1].trigger_px, Some(89000.0));
        assert_eq!(orders[1].tpsl, None);
    }

    #[test]
    fn order_status_mapping_never_guesses_a_fill() {
        assert_eq!(map_order_status("open"), OrderStatus::Open);
        assert_eq!(map_order_status("filled"), OrderStatus::Filled);
        assert_eq!(map_order_status("canceled"), OrderStatus::Canceled);
        assert_eq!(map_order_status("marginCanceled"), OrderStatus::Canceled);
        assert_eq!(map_order_status("selfTradeCanceled"), OrderStatus::Canceled);
        assert_eq!(map_order_status("rejected"), OrderStatus::Rejected);
        assert_eq!(map_order_status("tickRejected"), OrderStatus::Rejected);
        assert_eq!(map_order_status("badAloPxRejected"), OrderStatus::Rejected);
        assert_eq!(map_order_status("triggered"), OrderStatus::Open);
        assert_eq!(map_order_status("somethingNew"), OrderStatus::Unresolved);
    }

    #[test]
    fn order_status_response_parses_the_docs_example() {
        // The docs' orderStatus example: a market order, fully filled
        // (sz 0.0 of origSz 0.0076), cloid null.
        let v = serde_json::json!({
            "status": "order",
            "order": {
                "order": {
                    "coin": "ETH", "side": "A", "limitPx": "2412.7",
                    "sz": "0.0", "oid": 1, "timestamp": 1724361546645i64,
                    "triggerCondition": "N/A", "isTrigger": false,
                    "triggerPx": "0.0", "children": [],
                    "isPositionTpsl": false, "reduceOnly": true,
                    "orderType": "Market", "origSz": "0.0076",
                    "tif": "FrontendMarket", "cloid": null
                },
                "status": "filled",
                "statusTimestamp": 1724361546645i64
            }
        });
        let info =
            parse_order_status_response(&v, "hyperliquid", "acc-1", "0xabc", Some(2410.0)).unwrap();
        assert_eq!(info.status, OrderStatus::Filled);
        assert_eq!(info.order_id, "1");
        assert_eq!(info.client_order_id, "0xabc");
        assert_eq!(info.symbol, "ETH");
        assert_eq!(info.side, "sell");
        assert!((info.quantity - 0.0076).abs() < 1e-9);
        assert!((info.filled_quantity - 0.0076).abs() < 1e-9);
        assert!((info.remaining_quantity - 0.0).abs() < 1e-9);
        assert_eq!(info.average_fill_price, Some(2410.0));
        assert_eq!(info.created_at, 1724361546645i64);
    }

    #[test]
    fn order_status_unknown_oid_is_an_error_not_a_state() {
        let v = serde_json::json!({"status": "unknownOid"});
        let err =
            parse_order_status_response(&v, "hyperliquid", "acc-1", "0xabc", None).unwrap_err();
        assert_eq!(
            err,
            VenueError::rejected(crate::core::error::UNKNOWN_OID.to_string())
        );
    }

    #[test]
    fn order_status_partial_fill_becomes_partially_filled() {
        let v = serde_json::json!({
            "status": "order",
            "order": {
                "order": {
                    "coin": "io:BTC", "side": "B", "limitPx": "90000.0",
                    "sz": "0.004", "oid": 42, "timestamp": 1724361546645i64,
                    "triggerCondition": "N/A", "isTrigger": false,
                    "triggerPx": "0.0", "children": [],
                    "isPositionTpsl": false, "reduceOnly": false,
                    "orderType": "Limit", "origSz": "0.01",
                    "tif": "Gtc", "cloid": "0x1234567890abcdef1234567890abcdef"
                },
                "status": "open",
                "statusTimestamp": 1724361546700i64
            }
        });
        let info = parse_order_status_response(
            &v,
            "entropy",
            "acc-1",
            "0x1234567890abcdef1234567890abcdef",
            Some(90100.0),
        )
        .unwrap();
        assert_eq!(info.status, OrderStatus::PartiallyFilled);
        assert_eq!(info.symbol, "io:BTC", "the exchange coin is kept verbatim");
        assert_eq!(info.side, "buy");
        assert!((info.filled_quantity - 0.006).abs() < 1e-9);
        assert!((info.remaining_quantity - 0.004).abs() < 1e-9);
        assert_eq!(info.average_fill_price, Some(90100.0));
        assert_eq!(info.order_type, "limit");
    }

    #[test]
    fn open_orders_parse_the_docs_example() {
        let v = serde_json::json!([{
            "coin": "BTC", "limitPx": "29792.0", "oid": 91490942,
            "side": "A", "sz": "0.0", "timestamp": 1681247412573i64
        }]);
        let orders = parse_open_orders(&v, "hyperliquid", "acc-1").unwrap();
        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0].order_id, "91490942");
        assert_eq!(orders[0].side, "sell");
        assert_eq!(orders[0].status, OrderStatus::Open);
        assert!((orders[0].price.unwrap() - 29792.0).abs() < 1e-9);
    }

    #[test]
    fn fills_parse_and_match_by_oid() {
        let v = serde_json::json!([
            {"coin": "BTC", "px": "90000.0", "sz": "0.004", "side": "B",
             "time": 1724361546700i64, "oid": 42, "fee": "0.01",
             "feeToken": "USDC", "tid": 111, "hash": "0x00"},
            {"coin": "BTC", "px": "90100.0", "sz": "0.006", "side": "B",
             "time": 1724361546800i64, "oid": 43, "fee": "0.02",
             "feeToken": "USDC", "tid": 112, "hash": "0x00"}
        ]);
        let fills = parse_fills(&v, "hyperliquid", "acc-1", "0xabc", 42);
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].trade_id, "111");
        assert_eq!(fills[0].order_id, "42");
        assert_eq!(fills[0].client_order_id, "0xabc");
        assert!((fills[0].price - 90000.0).abs() < 1e-9);
        assert!((fills[0].fee.unwrap() - 0.01).abs() < 1e-9);
        assert_eq!(fills[0].fee_currency.as_deref(), Some("USDC"));
    }

    #[test]
    fn wire_strings_are_decimal_exact() {
        assert_eq!(decimal_to_wire(90000.0).unwrap(), "90000");
        assert_eq!(decimal_to_wire(0.01).unwrap(), "0.01");
        assert_eq!(decimal_to_wire(100.50).unwrap(), "100.5");
        assert!(decimal_to_wire(0.0000000001).is_err());
    }

    /// `openOrders` rows carry the cloid when the order was placed
    /// with one (null for website orders). The parser must keep it:
    /// without it a website / restarted-session order can only be
    /// cancelled by oid, and its post-cancel reconcile has no cloid
    /// to query by.
    #[test]
    fn parse_open_orders_keeps_the_cloid() {
        let v = serde_json::json!([
            {"coin": "io:NVDA", "side": "B", "limitPx": "90000.0", "sz": "0.01",
             "oid": 77_738_308, "timestamp": 1724361546700i64,
             "cloid": "0x1234567890abcdef1234567890abcdef"},
            {"coin": "ETH", "side": "A", "limitPx": "3000.0", "sz": "0.5",
             "oid": 77_738_309, "timestamp": 1724361546700i64, "cloid": null}
        ]);
        let orders = parse_open_orders(&v, "hyperliquid", "acc-1").expect("parse");
        assert_eq!(orders.len(), 2);
        assert_eq!(orders[0].order_id, "77738308");
        assert_eq!(
            orders[0].client_order_id,
            "0x1234567890abcdef1234567890abcdef"
        );
        // Rule 2.2: the exchange coin keeps its dex prefix.
        assert_eq!(orders[0].symbol, "io:NVDA");
        assert_eq!(orders[1].order_id, "77738309");
        assert_eq!(orders[1].client_order_id, "");
    }

    #[test]
    fn dex_offsets_match_the_official_sdk() {
        let names = perp_dex_names(PERP_DEXS_MAINNET_JSON);
        assert_eq!(names.len(), 10);
        assert_eq!(names.first().unwrap(), "xyz");
        assert_eq!(names.last().unwrap(), "io");
        assert_eq!(dex_offset_for(&names, "xyz"), Some(110_000));
        assert_eq!(dex_offset_for(&names, "io"), Some(200_000));
        // The old formula, for the record: io at 190000 (wrong).
        let old_offset = |pos: usize| 110_000 + (pos.saturating_sub(1) as u32) * 10_000;
        let io_pos = names.iter().position(|n| n == "io").unwrap();
        assert_eq!(old_offset(io_pos), 190_000);
        // Testnet: 289 raw entries, the single null at [0].
        let testnet = perp_dex_names(PERP_DEXS_TESTNET_JSON);
        assert_eq!(testnet.len(), 288);
        assert_eq!(testnet.last().unwrap(), "div");
        let div_pos = testnet.len() - 1;
        assert_eq!(
            dex_offset_for(&testnet, "div"),
            Some(110_000 + div_pos as u32 * 10_000)
        );
        // Every dex past the first was exactly 10000 short.
        for (pos, name) in testnet.iter().enumerate().skip(1) {
            assert_eq!(
                dex_offset_for(&testnet, name).unwrap() - old_offset(pos),
                10_000
            );
        }
    }
}
