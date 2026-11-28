import { create } from "zustand";
import { api } from "../lib/tauri";
import type { VenueSchema } from "../lib/types";
import { VENUE_DISPLAY_NAMES } from "../lib/venues";
import { useDataUiStore } from "./useDataUiStore";

/**
 * Venue schemas (Phase 2, I10): the single source for the credential
 * form. Fetched once at startup from `get_venue_schemas` and cached
 * here — no component keeps its own copy, and nothing in src/ lists
 * exchanges or their fields anymore.
 *
 * `names` is the id → display-name map for the tables; before the
 * schemas arrive it is empty and callers fall back to the id itself.
 * On first load the Data page's exchange filter is initialized to
 * all schema ids (it starts empty so a venue added later is checked
 * by default, not silently excluded).
 */
type S = {
  schemas: VenueSchema[] | null;
  names: Record<string, string>;
  loading: boolean;
  error: string | null;
  fetchSchemas: () => Promise<void>;
};

export const useVenueStore = create<S>((set, get) => ({
  schemas: null,
  names: {},
  loading: false,
  error: null,
  fetchSchemas: async () => {
    if (get().schemas !== null || get().loading) return;
    set({ loading: true, error: null });
    try {
      // One name map for the whole app (F4.3): backend schema names
      // are overridden here (e.g. "Lighter RH" → "RH Lighter"), so
      // every consumer — tables, dropdowns, toasts — sees the same
      // six full names without its own mapping.
      const schemas = (await api.getVenueSchemas()).map((s) => ({
        ...s,
        name: VENUE_DISPLAY_NAMES[s.id] ?? s.name,
      }));
      const names: Record<string, string> = {};
      for (const s of schemas) names[s.id] = s.name;
      set({ schemas, names, loading: false });
      useDataUiStore.getState().initChecked(schemas.map((s) => s.id));
    } catch (e) {
      set({
        loading: false,
        error: e instanceof Error ? e.message : String(e),
      });
    }
  },
}));
