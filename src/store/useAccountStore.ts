import { create } from "zustand";
import { api } from "../lib/tauri";
import type { AccountView, CredentialMap } from "../lib/types";
import { useConsoleStore } from "./useConsoleStore";
import { useLiveStore } from "./useLiveStore";

/**
 * Account metadata only. Credentials are never stored here — they exist
 * briefly in the modal's local state during submit and then only in the
 * Rust vault. No persistence middleware is used on purpose.
 *
 * Background failures (list load, vault status, delete) go to the bottom
 * console only — never a banner above the list. `error` is kept solely
 * for the add/edit modal, which shows it inside the dialog.
 *
 * Live sync lives here too (Phase 2, I11): hydrate + auto-connect run
 * with the list load, so the page only reads state. Connect failures
 * are logged to the console by the backend; nothing is shown here so
 * the list never shifts.
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
  connect: (id: string) => Promise<void>;
  disconnect: (id: string) => Promise<void>;
  hydrateLive: () => Promise<void>;
};

function errText(e: unknown): string {
  return typeof e === "string" ? e : e instanceof Error ? e.message : "未知错误";
}

function consoleError(message: string) {
  useConsoleStore.getState().addLog({ ts: Date.now(), level: "error", source: "DeSSS", message });
}

/** Last vault-init error already reported, so a refresh does not re-log it. */
let lastVaultErrorLogged: string | null = null;

/** Accounts already tried this session: a failed auto-connect is never
 * retried by the list reload, only by the manual 连接 button. */
const connectTried = new Set<string>();

export const useAccountStore = create<AccountState>((set, get) => {
  /** Connect every credentialed account that is not already live.
   * Runs after the list + live hydration; also after create/update
   * (they reload the list). Private keys are never involved: the
   * backend only reads the public address / account index from the
   * vault. */
  const autoConnect = () => {
    const live = useLiveStore.getState().live;
    for (const a of get().accounts) {
      if (!a.credentialConfigured) continue;
      if (live[a.id] || connectTried.has(a.id)) continue;
      connectTried.add(a.id);
      void get().connect(a.id);
    }
  };

  return {
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
        await get().hydrateLive();
        autoConnect();
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
        connectTried.delete(id);
        await get().fetchAll();
        return true;
      } catch (e) {
        consoleError(`删除账户失败：${errText(e)}`);
        return false;
      }
    },
    connect: async (id) => {
      try {
        const a = await api.connectAccount(id);
        useLiveStore.getState().setLive(a);
      } catch {
        // Backend logs the failure; the list never shifts.
      }
    },
    disconnect: async (id) => {
      await api.disconnectAccount(id).catch(() => {});
      useLiveStore.getState().removeLive(id);
    },
    hydrateLive: async () => {
      try {
        const list = await api.listAccountLive();
        list.forEach((a) => useLiveStore.getState().setLive(a));
      } catch {
        // Live states hydrate again on the next list load.
      }
    },
  };
});
