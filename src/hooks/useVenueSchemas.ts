import { useEffect } from "react";
import { useVenueStore } from "../store/useVenueStore";

/**
 * Venue schemas for the account form and the tables (Phase 2, I10).
 * Triggers the one-time fetch from `get_venue_schemas` on first use;
 * every caller shares the cached store, so mounting this in several
 * places still fetches exactly once.
 */
export function useVenueSchemas() {
  const schemas = useVenueStore((s) => s.schemas);
  const loading = useVenueStore((s) => s.loading);
  const error = useVenueStore((s) => s.error);
  const fetchSchemas = useVenueStore((s) => s.fetchSchemas);
  useEffect(() => {
    void fetchSchemas();
  }, [fetchSchemas]);
  return { schemas, loading, error };
}
