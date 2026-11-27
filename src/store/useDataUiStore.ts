import { create } from "zustand";

/** Data page view: which half of the page is shown. */
export type DataView = "search" | "signals";

/**
 * Data page view state lives here (not in DataPage) so the toolbar
 * toggle registered via useShellActions reads it, like the account
 * page's "添加账户" button in the same slot. The search query and
 * checked exchanges live here too (PR-0.3), so switching pages never
 * loses the search.
 *
 * `checked` starts empty and is filled with every schema id on the
 * first venue-schema load (Phase 2, I10): a venue added later is
 * checked by default instead of silently excluded. `initChecked`
 * runs once — the user's own selection after that is never reset.
 */
type S = {
  view: DataView;
  setView: (v: DataView) => void;
  query: string;
  setQuery: (q: string) => void;
  checked: string[];
  checkedInitialized: boolean;
  setChecked: (ids: string[]) => void;
  initChecked: (ids: string[]) => void;
};

export const useDataUiStore = create<S>((set, get) => ({
  view: "search",
  setView: (view) => set({ view }),
  query: "",
  setQuery: (query) => set({ query }),
  checked: [],
  checkedInitialized: false,
  setChecked: (checked) => set({ checked }),
  initChecked: (ids) => {
    if (get().checkedInitialized) return;
    set({ checked: ids, checkedInitialized: true });
  },
}));
