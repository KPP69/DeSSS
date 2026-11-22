use serde::{Serialize,Deserialize};
#[derive(Clone,Debug,Serialize,Deserialize)] #[serde(rename_all="camelCase")]
pub struct Tick{ pub adapter_id:String, pub symbol:String, pub bid:f64, pub ask:f64, pub last:f64, pub volume24h:f64, pub ts:i64 }
#[derive(Clone,Debug,Serialize,Deserialize)] #[serde(rename_all="camelCase")]
pub struct Balance{ pub adapter_id:String, pub account_id:String, pub asset:String, pub free:f64, pub locked:f64, pub total:f64, pub equity_usd:f64, pub ts:i64 }
#[derive(Clone,Debug,Serialize,Deserialize)] #[serde(rename_all="camelCase")]
pub struct Position{ pub adapter_id:String, pub account_id:String, pub symbol:String, pub side:String, pub qty:f64, pub entry_price:f64, pub mark_price:f64, pub unrealized_pnl:f64 }
#[derive(Clone,Debug,Serialize,Deserialize)] #[serde(rename_all="camelCase")]
pub struct OrderRequest{ pub adapter_id:String, pub account_id:String, pub symbol:String, pub side:String, pub order_type:String, pub qty:f64, pub price:Option<f64>, pub client_order_id:Option<String> }
#[derive(Clone,Debug,Serialize,Deserialize)] #[serde(rename_all="camelCase")]
pub struct Order{ pub id:String, pub adapter_id:String, pub account_id:String, pub symbol:String, pub side:String, pub order_type:String, pub qty:f64, pub price:Option<f64>, pub status:String, pub ts:i64 }
