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

/** Credential values keyed by the field keys of the backend venue
 * schemas (`get_venue_schemas`); the frontend holds no field table. */
export type CredentialMap = Record<string, string | number>;

/** One credential field as the backend venue schema defines it
 * (Phase 2, I10). The account form renders exactly this. */
export type VenueSchemaField = {
  key: string;
  label: string;
  kind: "text" | "password" | "number";
  required: boolean;
  min?: number;
  max?: number;
  forbidden?: number[];
  hint?: string;
};

/** One venue's form definition from `get_venue_schemas`. */
export type VenueSchema = {
  id: string;
  name: string;
  note?: string;
  /** Phase 3.1: the venue can take orders (HL / Entropy only). */
  supportsTrading: boolean;
  fields: VenueSchemaField[];
};

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
  pnlUsd: number | null;
  currency: string;
  positions: LivePosition[];
  error: string | null;
  /** "ws" | "rest" — where this snapshot came from (Phase 3.3) */
  source: "ws" | "rest";
  /** true while the account WS is down or silent: REST fallback (Phase 3.3) */
  degraded: boolean;
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
  change24h: number | null;
  /** When the quote fields were last written, unix ms (PR-B1); null
   * until the first bulk or quote refresh (e.g. after a restart). */
  quoteAt: number | null;
  updatedAt: number;
};

/** Result of a manual catalog refresh: cached rows + refresh timestamp. */
export type CatalogRefresh = { count: number; updatedAt: number };

/** Per-venue result of a batch quote refresh (PR-B1). */
export type QuoteVenueResult = { exchangeId: string; ok: boolean; rows: number; error: string | null };

/** Per-venue catalog status (PR-0.3): row count, newest row time, last error. */
export type VenueCatalogStatus = { exchangeId: string; updatedAt: number | null; rows: number; error: string | null };
export type CatalogStatus = { venues: VenueCatalogStatus[]; logDir: string; dataDir: string };

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
  /** Trigger price for TP/SL orders, null otherwise. */
  triggerPx: number | null;
  /** Trigger purpose: "tp" | "sl", null for non-trigger orders. */
  tpsl: string | null;
  /** Where the order came from: "manual", or "strategy:<id>". */
  origin: string;
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

/** Spread strategy (Phase 3.1): one leg = one account row. HL main
 * and Entropy are two rows of the same wallet. */
export type LegRef = { accountId: string; exchangeId: string; symbol: string };

/** Mean-reversion spread parameters (all bps unless noted). */
export type SpreadParams = {
  midlineBps: number;
  upperBps: number;
  lowerBps: number;
  persistSec: number;
  takeFraction: number;
  maxNotionalUsd: number;
  feeBpsA: number;
  feeBpsB: number;
  tpBps: number;
  slBps: number;
};

export type StrategyInstance = {
  id: string;
  kind: string;
  legA: LegRef;
  legB: LegRef;
  params: SpreadParams;
  enabled: boolean;
  createdAt: number;
};

/** One instance plus its live view state (Phase 3.1, commit 2). */
export type StrategyView = StrategyInstance & {
  lastEvalMs: number | null;
  lastError: string | null;
  ordersPlaced: number;
};

export type SaveStrategyInput = {
  id?: string | null;
  kind: string;
  legA: LegRef;
  legB: LegRef;
  params: SpreadParams;
  enabled: boolean;
};

export type SaveStrategyResult = { instance: StrategyInstance; warning: string | null };
