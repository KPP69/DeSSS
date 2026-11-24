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

/** Live read-only state of one connected real account (phase 3). */
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
