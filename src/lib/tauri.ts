import { invoke } from "@tauri-apps/api/core"; import { listen, type UnlistenFn } from "@tauri-apps/api/event"; import type { Tick, Balance, AppInfo } from "./types";
export const api={ getAppInfo:()=>invoke<AppInfo>("get_app_info"), startMock:()=>invoke<void>("start_mock"), stopMock:()=>invoke<void>("stop_mock"), getSnapshot:()=>invoke<{ticks:Tick[];balances:Balance[]}>("get_snapshot") };
export function onTick(cb:(t:Tick)=>void):Promise<UnlistenFn>{ return listen<Tick>("market://tick",e=>cb(e.payload)); }
export function onBalance(cb:(b:Balance)=>void):Promise<UnlistenFn>{ return listen<Balance>("account://balance",e=>cb(e.payload)); }
