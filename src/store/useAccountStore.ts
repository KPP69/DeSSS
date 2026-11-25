import { create } from "zustand";
import { api } from "../lib/tauri";
import type { AccountView, CredentialMap } from "../lib/types";
import { useConsoleStore } from "./useConsoleStore";

/**
 * Account metadata only. Credentials are never stored here — they exist
 * briefly in the modal's local state during submit and then only in the
 * Rust vault. No persistence middleware is used on purpose.
 *
 * Background failures (list load, vault status, delete) go to the bottom
 * console only — never a banner above the list. `error` is kept solely
 * for the add/edit modal, which shows it inside the dialog.
 */
type AccountState = {
  accounts: AccountView[];
  loading: boolean;
  error: string | null;
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

function consoleError(message: string) {
  useConsoleStore.getState().addLog({ ts: Date.now(), level: "error", source: "DeSSS", message });
}

/** Last vault-init error already reported, so a refresh does not re-log it. */
let lastVaultErrorLogged: string | null = null;

export const useAccountStore = create<AccountState>((set, get) => ({
  accounts: [],
  loading: false,
  error: null,
  fetchAll: async () => {
    set({ loading: true });
    try {
      const [accounts, vaultError] = await Promise.all([
        api.listAccounts(),
        api.getVaultStatus().catch(() => null),
      ]);
      if (vaultError && vaultError !== lastVaultErrorLogged) {
        lastVaultErrorLogged = vaultError;
        consoleError(
          `Vault 初始化失败，账户凭证功能不可用：${vaultError}（原有 vault 文件未被修改，请检查 data 目录下的 vault.key / vault.enc）`
        );
      } else if (!vaultError) {
        lastVaultErrorLogged = null;
      }
      set({ accounts, error: null, loading: false });
    } catch (e) {
      consoleError(`加载账户列表失败：${errText(e)}`);
      set({ loading: false });
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
      consoleError(`删除账户失败：${errText(e)}`);
      return false;
    }
  },
}));
