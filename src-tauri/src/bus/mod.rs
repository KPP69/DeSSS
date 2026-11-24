use tokio::sync::broadcast; use crate::adapters::types::*;
use serde::{Serialize,Deserialize};
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag="kind", rename_all="camelCase")]
pub enum AppEvent { Tick(Tick), Balance(Balance), Position(Position), Book(OrderBook), AccountLive(AccountLive), OrderUpdate(Order), Log{level:String,message:String}, System{message:String} }
#[derive(Clone)]
pub struct EventBus { tx: broadcast::Sender<AppEvent> }
impl EventBus { pub fn new(capacity:usize)->Self{ let (tx,_)=broadcast::channel(capacity); Self{tx} } pub fn publish(&self, e:AppEvent){ let _=self.tx.send(e); } pub fn subscribe(&self)->broadcast::Receiver<AppEvent>{ self.tx.subscribe() } }
impl Default for EventBus { fn default()->Self{ Self::new(1024) } }
