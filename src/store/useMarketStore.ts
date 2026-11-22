import { create } from "zustand"; import type { Tick, Balance } from "../lib/types";
type S={ticks:Record<string,Tick>; balances:Balance[]; connected:boolean; setTick:(t:Tick)=>void; setBalance:(b:Balance)=>void; setConnected:(v:boolean)=>void};
export const useMarketStore=create<S>((set)=>({ticks:{},balances:[],connected:false,setTick:(t)=>set(s=>({ticks:{...s.ticks,[t.symbol]:t}})),setBalance:(b)=>set(s=>({balances:[...s.balances.filter(x=>x.asset!==b.asset||x.accountId!==b.accountId),b]})),setConnected:(v)=>set({connected:v})}));
