import { create } from "zustand";
import { api } from "../lib/tauri";
import type { AccountView, CredentialMap } from "../lib/types";

/**
 * Account metadata only. Credentials are never stored here — they exist
 * briefly in the modal's local state during submit and then only in the
 * Rust vault. No persistence middleware is used on purpose.
 */
type AccountState = {
  accounts: AccountView[];
  loading: boolean;
  error: string | null;
  vaultError: string | null;
  fetchAll: () => Promise<void>;
  create: (label: string, exchangeId: string, credentials: CredentialMap) => Promise<boolean>;
  update: (
    id: string,
    label: string,
    exchangeId: string,
    credentials: CredentialMap | null
  ) => Promise<boolean>;
  remove: (id: string) => Promise<boolean>;
};

function errText(e: unknown): string {
  return typeof e === "string" ? e : e instanceof Error ? e.message : "未知错误";
}

export const useAccountStore = create<AccountState>((set, get) => ({
  accounts: [],
  loading: false,
  error: null,
  vaultError: null,
  fetchAll: async () => {
    set({ loading: true });
    try {
      const [accounts, vaultError] = await Promise.all([
        api.listAccounts(),
        api.getVaultStatus().catch(() => null),
      ]);
      set({ accounts, vaultError, error: null, loading: false });
    } catch (e) {
      set({ error: errText(e), loading: false });
    }
  },
  create: async (label, exchangeId, credentials) => {
    try {
      await api.createAccount(label, exchangeId, credentials);
      await get().fetchAll();
      return true;
    } catch (e) {
      set({ error: errText(e) });
      return false;
    }
  },
  update: async (id, label, exchangeId, credentials) => {
    try {
      await api.updateAccount(id, label, exchangeId, credentials);
      await get().fetchAll();
      return true;
    } catch (e) {
      set({ error: errText(e) });
      return false;
    }
  },
  remove: async (id) => {
    try {
      await api.deleteAccount(id);
      await get().fetchAll();
      return true;
    } catch (e) {
      set({ error: errText(e) });
      return false;
    }
  },
}));
