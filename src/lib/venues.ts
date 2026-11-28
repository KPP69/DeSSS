/**
 * The one venue display-name map for the whole app (F4.3). Every
 * table, dropdown and toast reads names through here (via the venue
 * store, which overrides the backend schema names with this map on
 * load) — never a second per-page map. Backend names differ
 * ("Lighter RH"); the UI always shows these six full names.
 */
export const VENUE_DISPLAY_NAMES: Record<string, string> = {
  hyperliquid: "Hyperliquid",
  entropy: "Entropy",
  lighter: "Lighter",
  lighter_robinhood: "RH Lighter",
  asterdex: "AsterDEX",
  arcus: "Arcus",
};

export const venueDisplayName = (id: string, fallback?: string): string =>
  VENUE_DISPLAY_NAMES[id] ?? fallback ?? id;
