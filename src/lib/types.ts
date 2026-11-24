export type Tick = { adapterId: string; symbol: string; bid: number; ask: number; last: number; volume24h: number; ts: number };
export type Balance = { adapterId: string; accountId: string; asset: string; free: number; locked: number; total: number; equityUsd: number; ts: number };
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
  /** "connecting" | "connected" | "reconnecting" | "error" | "disconnected" */
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
