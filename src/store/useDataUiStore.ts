import { create } from "zustand";
import { EXCHANGES } from "../lib/exchanges";

/** Data page view: which half of the page is shown. */
export type DataView = "search" | "signals";

/**
 * Data page view state lives here (not in DataPage) so the AppShell
 * toolbar owns the view toggle, like the account page's "添加账户"
 * button in the same slot. The search query and checked exchanges
 * live here too (PR-0.3), so switching pages never loses the search.
 */
type S = {
  view: DataView;
  setView: (v: DataView) => void;
  query: string;
  setQuery: (q: string) => void;
  checked: string[];
  setChecked: (ids: string[]) => void;
};

export const useDataUiStore = create<S>((set) => ({
  view: "search",
  setView: (view) => set({ view }),
  query: "",
  setQuery: (query) => set({ query }),
  checked: EXCHANGES.map((e) => e.id),
  setChecked: (checked) => set({ checked }),
}));
