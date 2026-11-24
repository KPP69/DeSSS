import { create } from "zustand";
import type { AccountView } from "../lib/types";

/**
 * Account modal state lives here (not in AccountPage) so the AppShell
 * toolbar's "添加账户" button can open the modal.
 */
type S = {
  modalOpen: boolean;
  editing: AccountView | null;
  openAdd: () => void;
  openEdit: (a: AccountView) => void;
  close: () => void;
};

export const useAccountUiStore = create<S>((set) => ({
  modalOpen: false,
  editing: null,
  openAdd: () => set({ modalOpen: true, editing: null }),
  openEdit: (a) => set({ modalOpen: true, editing: a }),
  close: () => set({ modalOpen: false }),
}));
