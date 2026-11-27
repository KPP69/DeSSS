import { invoke as tauriInvoke } from "@tauri-apps/api/core";
import { listen, type UnlistenFn } from "@tauri-apps/api/event";
import type { AppInfo, AccountView, CredentialMap, OrderBook, AccountLive, LogEntry, RuntimeSnapshot, ProxyState, MarketInfo, CatalogRefresh, CatalogStatus, RiskConfig, DrawdownStatus, OrderInfo, TradeInfo, VenueSchema, StrategyView, StrategyInstance, SaveStrategyInput, SaveStrategyResult } from "./types";

// Tauri 框架级英文报错翻译成中文（实机事故 2026-11-26：下单失败只显示
// "invalid args input for command place_order: missing field account_id"，
// 用户看不懂）。业务错误（后端返回的中文，如风控拒绝、下单被拒）原样
// 透传；只有框架级报错（参数解析、未知字段、命令不存在）才翻译。
const FIELD_NAMES: Record<string, string> = {
  accountId: "账户",
  orderId: "订单号",
  clientOrderId: "客户端订单号",
  orderType: "订单类型",
  exchangeId: "交易所",
  symbol: "交易对",
  quantity: "数量",
  price: "价格",
  side: "方向",
  killSwitch: "风控总开关",
};

function describeInvokeError(command: string, err: unknown): string {
  const msg = err instanceof Error ? err.message : String(err);
  const missing = msg.match(/missing field `?([A-Za-z0-9_]+)`?/);
  if (msg.includes("invalid args input for command") && missing) {
    const field = missing[1];
    const label = FIELD_NAMES[field] ?? field;
    return `调用 ${command} 的参数不完整：缺少「${label}」（${field}）。请重试；如果一直出现，请把这条提示发给开发者`;
  }
  if (msg.includes("invalid args input for command")) {
    return `调用 ${command} 的参数不正确，可能是前后端字段不一致。请重试；如果一直出现，请把这条提示发给开发者`;
  }
  const unknown = msg.match(/unknown field `?([A-Za-z0-9_]+)`?/);
  if (unknown) {
    return `调用 ${command} 时传了后端不认识的字段 ${unknown[1]}，请更新到最新版本后重试`;
  }
  if (msg.includes("command not found")) {
    return `命令 ${command} 不存在，请更新到最新版本后重试`;
  }
  return msg;
}

async function invoke<T>(command: string, args?: Record<string, unknown>): Promise<T> {
  try {
    return await tauriInvoke<T>(command, args);
  } catch (e) {
    throw new Error(describeInvokeError(command, e));
  }
}

export const api = {
  getAppInfo: () => invoke<AppInfo>("get_app_info"),
  getRuntimeSnapshot: () => invoke<RuntimeSnapshot>("get_runtime_snapshot"),
  getProxy: () => invoke<ProxyState>("get_proxy"),
  setProxy: (url: string) => invoke<ProxyState>("set_proxy", { url }),
  // Venue schemas (Phase 2, I10): the credential-form definition,
  // straight from the backend registry. The account form renders
  // exactly this; adding a field in Rust shows up with no
  // frontend change.
  getVenueSchemas: () => invoke<VenueSchema[]>("get_venue_schemas"),
  // Market catalog (PR-0.3): search is zero-network — it reads the
  // SQLite-backed in-memory catalog only. A miss triggers one
  // background refresh; the page re-searches on `catalog://updated`.
  // The refresh button fires a background refresh and returns at once.
  searchMarketInfo: (query: string, exchangeIds: string[]) =>
    invoke<MarketInfo[]>("search_market_info", { query, exchangeIds }),
  refreshMarketCatalog: (exchangeIds: string[]) =>
    invoke<CatalogRefresh>("refresh_market_catalog", { exchangeIds }),
  getCatalogStatus: () => invoke<CatalogStatus>("get_catalog_status"),
  // Realtime streams (phase 5A §2): reconcile the backend's wanted set
  // with the symbols on screen. Empty list = unsubscribe everything.
  setStreamSymbols: (keys: { exchangeId: string; symbol: string }[]) =>
    invoke<void>("set_stream_symbols", { keys }),
  // Risk gateway (phase 5A §3): kill switch + hard limits. The toggle
  // takes effect immediately; limits are validated backend-side.
  getRiskConfig: () => invoke<RiskConfig>("get_risk_config"),
  setRiskConfig: (config: RiskConfig) =>
    invoke<RiskConfig>("set_risk_config", { config }),
  setKillSwitch: (enabled: boolean) =>
    invoke<RiskConfig>("set_kill_switch", { enabled }),
  // Drawdown snapshot for the Risk page (sampled every 10s + at each
  // order attempt).
  getDrawdownStatus: () => invoke<DrawdownStatus>("get_drawdown_status"),
  // Orders (phase 5A §4): manual trading test area + open orders.
  // place_order returns the order record; Unresolved is an Ok
  // result with `error` set, not a thrown error.
  placeOrder: (input: {
    accountId: string;
    symbol: string;
    side: string;
    orderType: string;
    quantity: number;
    price: number;
  }) => invoke<OrderInfo>("place_order", { input }),
  cancelOrder: (accountId: string, orderId: string, symbol: string) =>
    invoke<OrderInfo>("cancel_order", { accountId, orderId, symbol }),
  listOpenOrders: (accountId: string) =>
    invoke<OrderInfo[]>("list_open_orders", { accountId }),
  listOrderRecords: () => invoke<OrderInfo[]>("list_order_records"),
  listOrderFills: () => invoke<TradeInfo[]>("list_order_fills"),
  // Strategy (Phase 3.1): spread instances. save returns the stored
  // instance plus a midline warning when one applies (a warning, not
  // an error — the save succeeds).
  listStrategies: () => invoke<StrategyView[]>("list_strategies"),
  saveStrategy: (input: SaveStrategyInput) =>
    invoke<SaveStrategyResult>("save_strategy", { input }),
  setStrategyEnabled: (id: string, enabled: boolean) =>
    invoke<StrategyInstance>("set_strategy_enabled", { id, enabled }),
  // Account center (phase 2). Credentials go to the Rust vault exactly once;
  // nothing secret is ever returned.
  listAccounts: () => invoke<AccountView[]>("list_accounts"),
  createAccount: (label: string, exchangeId: string, credentials: CredentialMap) =>
    invoke<AccountView>("create_account", { label, exchangeId, credentials }),
  updateAccount: (id: string, label: string, exchangeId: string, credentials: CredentialMap | null) =>
    invoke<AccountView>("update_account", { id, label, exchangeId, credentials }),
  deleteAccount: (id: string) => invoke<void>("delete_account", { id }),
  getVaultStatus: () => invoke<string | null>("get_vault_status"),
  // Real read-only exchange sync (phase 3). The backend reads the address /
  // account index from the vault; private keys are never used or returned.
  connectAccount: (id: string) => invoke<AccountLive>("connect_account", { id }),
  disconnectAccount: (id: string) => invoke<void>("disconnect_account", { id }),
  listAccountLive: () => invoke<AccountLive[]>("list_account_live"),
};

export function onBook(cb: (b: OrderBook) => void): Promise<UnlistenFn> {
  return listen<OrderBook>("market://book", (e) => cb(e.payload));
}
export function onCatalogUpdated(cb: () => void): Promise<UnlistenFn> {
  return listen("catalog://updated", () => cb());
}
export function onAccountLive(cb: (a: AccountLive) => void): Promise<UnlistenFn> {
  return listen<AccountLive>("account://live", (e) => cb(e.payload));
}
export function onLog(cb: (l: LogEntry) => void): Promise<UnlistenFn> {
  return listen<LogEntry>("app://log", (e) => cb(e.payload));
}
