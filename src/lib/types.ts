export type Position = { adapterId: string; accountId: string; symbol: string; side: string; qty: number; entryPrice: number; markPrice: number; unrealizedPnl: number };
export type AppInfo = { name: string; version: string; dataDir: string; dbPath: string; portable: boolean };
export type LegBinding = { legId: string; adapterId: string; accountId: string; symbol: string; ratio: number; side: string; enabled: boolean };

/** Account metadata as returned by the Rust backend. Never contains secrets. */
export type AccountView = {
  id: string;
  label: string;
  exchangeId: string;
  maskedIdentifier: string;
  createdAt: number;
  updatedAt: number;
  status: string;
  credentialConfigured: boolean;
};

/** Credential values keyed by the field keys defined in lib/exchanges.ts. */
export type CredentialMap = Record<string, string | number>;

/** One price level of an order book: [price, quantity]. */
export type PriceLevel = [number, number];

export type OrderBook = { adapterId: string; symbol: string; bids: PriceLevel[]; asks: PriceLevel[]; ts: number };

export type LivePosition = { symbol: string; side: string; qty: number; entryPrice: number; markPrice: number; unrealizedPnl: number };

/** One runtime log line pushed by the Rust backend for the console drawer. */
export type LogEntry = {
  level: "info" | "warn" | "error" | string;
  source: string;
  message: string;
  ts: number;
};

/** Latest in-memory books + account states + recent logs, fetched once
 * on load so a refresh shows numbers instantly. */
export type RuntimeSnapshot = { books: OrderBook[]; live: AccountLive[]; logs: LogEntry[] };

/** Proxy configuration: the user-set value plus what is actually in effect. */
export type ProxyState = { custom: string | null; effective: string | null };

export type AccountLive = {
  accountId: string;
  adapterId: string;
  /** "connecting" | "connected" | "error" | "disconnected" */
  status: string;
  latencyMs: number | null;
  equityUsd: number;
  availableUsd: number;
  pnlUsd: number;
  currency: string;
  positions: LivePosition[];
  error: string | null;
  ts: number;
};

/** One standardized market row from the bulk catalog (phase 4). `null`
 * means the exchange has no public source for that field (e.g. AsterDEX
 * max leverage — there is no public endpoint for it — or an AsterDEX
 * funding interval that could not be confirmed, which also leaves the
 * 1h rate and the APR null). Bid/ask come only from a real order book;
 * a `null` here only means the per-symbol fill failed for that row. */
export type MarketInfo = {
  exchangeId: string;
  symbol: string;
  baseSymbol: string;
  settlementCurrency: string;
  maxLeverage: number | null;
  tickSize: number | null;
  stepSize: number | null;
  minNotional: number | null;
  bidPx: number | null;
  askPx: number | null;
  midPx: number | null;
  markPx: number | null;
  indexPx: number | null;
  fundingRateRaw: number | null;
  fundingIntervalHours: number | null;
  funding1h: number | null;
  fundingApr: number | null;
  nextFundingTime: number | null;
  volume24hUsd: number | null;
  openInterestUsd: number | null;
  updatedAt: number;
};

/** Result of a manual catalog refresh: cached rows + refresh timestamp. */
export type CatalogRefresh = { count: number; updatedAt: number };

/** Risk gateway config: kill switch + hard limits (phase 5A §3). */
export type RiskConfig = {
  killSwitch: boolean;
  maxOrderNotionalUsd: number;
  maxPositionUsd: number;
  maxDrawdownPct: number;
  allowedSides: "both" | "long_only" | "short_only";
  autoPauseAfterFailures: number;
};

/** Drawdown snapshot: session initial equity, high-water mark, latest
 * sampled equity and current drawdown against the high. */
export type DrawdownStatus = {
  initialEquity: number | null;
  sessionHigh: number;
  lastEquity: number | null;
  drawdownPct: number;
};

/** Canonical order status (snake_case wire names). */
export type OrderStatus =
  | "pending"
  | "submitted"
  | "open"
  | "partially_filled"
  | "filled"
  | "rejected"
  | "canceled"
  | "expired"
  | "failed"
  | "unresolved";

/** Canonical order record (phase 5A §3/§4). */
export type OrderInfo = {
  orderId: string;
  clientOrderId: string;
  exchangeId: string;
  accountId: string;
  symbol: string;
  side: string;
  orderType: string;
  status: OrderStatus;
  price: number | null;
  quantity: number;
  filledQuantity: number;
  remainingQuantity: number;
  averageFillPrice: number | null;
  error: string | null;
  createdAt: number;
  updatedAt: number;
};

/** One fill of an order. */
export type TradeInfo = {
  tradeId: string;
  exchangeId: string;
  orderId: string;
  clientOrderId: string;
  accountId: string;
  symbol: string;
  side: string;
  price: number;
  quantity: number;
  fee: number | null;
  feeCurrency: string | null;
  timestamp: number;
};
