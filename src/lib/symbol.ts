/**
 * Display-only symbol formatting (Phase 2).
 *
 * Entropy markets carry an "io:" prefix on the wire (`io:BTC`) so the
 * backend can tell the Entropy dex apart from mainnet Hyperliquid.
 * The tables strip it for display only — never use the result for a
 * comparison, a lookup, a request, or a store key: the prefixed form
 * is the identity, this is just how it prints.
 */
export function displaySymbol(symbol: string): string {
  return symbol.startsWith("io:") ? symbol.slice(3) : symbol;
}
