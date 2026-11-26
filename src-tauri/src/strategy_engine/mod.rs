use serde::{Serialize,Deserialize};
/// Strategy leg abstraction: 1 leg = single, N legs = multi-leg. Never hardcode A/B legs.
#[derive(Clone,Debug,Serialize,Deserialize)] #[serde(rename_all="camelCase")]
pub struct LegBinding { pub leg_id:String, pub adapter_id:String, pub account_id:String, pub symbol:String, pub ratio:f64, pub side:String, pub enabled:bool }
#[derive(Clone,Debug,Serialize,Deserialize)] #[serde(rename_all="camelCase")]
pub struct StrategyDefinition { pub id:String, pub name:String, pub legs: Vec<LegBinding>, pub enabled:bool }
#[async_trait::async_trait] pub trait Strategy: Send+Sync { fn id(&self)->&str; async fn on_tick(&self, legs:&[LegBinding], tick:&crate::core::types::Tick); }
