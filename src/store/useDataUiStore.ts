import { create } from "zustand";

/** Data page view: which half of the page is shown. */
export type DataView = "search" | "signals";

/**
 * Data page view state lives here (not in DataPage) so the AppShell
 * toolbar owns the view toggle, like the account page's "添加账户"
 * button in the same slot.
 */
type S = {
  view: DataView;
  setView: (v: DataView) => void;
};

export const useDataUiStore = create<S>((set) => ({
  view: "search",
  setView: (view) => set({ view }),
}));
