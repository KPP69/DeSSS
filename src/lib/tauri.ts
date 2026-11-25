import { invoke } from "@tauri-apps/api/core";
import { listen, type UnlistenFn } from "@tauri-apps/api/event";
import type { AppInfo, AccountView, CredentialMap, OrderBook, AccountLive, LogEntry, RuntimeSnapshot, ProxyState, MarketInfo, CatalogRefresh, RiskConfig, DrawdownStatus, OrderInfo, TradeInfo } from "./types";

export const api = {
  getAppInfo: () => invoke<AppInfo>("get_app_info"),
  getRuntimeSnapshot: () => invoke<RuntimeSnapshot>("get_runtime_snapshot"),
  getProxy: () => invoke<ProxyState>("get_proxy"),
  setProxy: (url: string) => invoke<ProxyState>("set_proxy", { url }),
  // Market catalog (phase 4): bulk specs + prices + funding for all six
  // exchanges, searched on demand (empty query returns nothing).
  // A search fetches only the checked exchanges whose cache is older
  // than 30s; the refresh button forces a refetch of the checked ones.
  searchMarketInfo: (query: string, exchangeIds: string[]) =>
    invoke<MarketInfo[]>("search_market_info", { query, exchangeIds }),
  refreshMarketCatalog: (exchangeIds: string[]) =>
    invoke<CatalogRefresh>("refresh_market_catalog", { exchangeIds }),
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
export function onAccountLive(cb: (a: AccountLive) => void): Promise<UnlistenFn> {
  return listen<AccountLive>("account://live", (e) => cb(e.payload));
}
export function onLog(cb: (l: LogEntry) => void): Promise<UnlistenFn> {
  return listen<LogEntry>("app://log", (e) => cb(e.payload));
}
