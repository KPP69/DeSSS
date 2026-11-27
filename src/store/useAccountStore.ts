import { create } from "zustand";
import { api } from "../lib/tauri";
import type { AccountView, CredentialMap } from "../lib/types";
import { useConsoleStore } from "./useConsoleStore";
import { useLiveStore } from "./useLiveStore";
import { toast } from "./useToastStore";

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
 * with the list load, so the page only reads state. A manual connect /
 * disconnect failure toasts the backend's own words (F1); the
 * background auto-connect stays silent so a bad credential does not
 * pop a toast on every load.
 */
type AccountState = {
  accounts: AccountView[];
  loading: boolean;
  error: string | null;
  /** Last list-load failure (F3): the page shows it as its error
   * state with a retry. Null on every successful load. */
  listError: string | null;
  fetchAll: () => Promise<void>;
  create: (label: string, exchangeId: string, credentials: CredentialMap) => Promise<boolean>;
  update: (
    id: string,
    label: string,
    exchangeId: string,
    credentials: CredentialMap | null
  ) => Promise<boolean>;
  remove: (id: string) => Promise<boolean>;
  /** True when the account is live afterwards (F3: the page toasts
   * success on true; failures already toast inside, unless silent). */
  connect: (id: string, silent?: boolean) => Promise<boolean>;
  /** True when the disconnect call succeeded. */
  disconnect: (id: string) => Promise<boolean>;
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
      void get().connect(a.id, true);
    }
  };

  return {
    accounts: [],
    loading: false,
    error: null,
    listError: null,
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
        set({ accounts, error: null, listError: null, loading: false });
        await get().hydrateLive();
        autoConnect();
      } catch (e) {
        consoleError(`加载账户列表失败：${errText(e)}`);
        set({ loading: false, listError: errText(e) });
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
        // User-initiated delete (F3): the failure toasts verbatim
        // (and still goes to the console, as before).
        consoleError(`删除账户失败：${errText(e)}`);
        toast.error(errText(e));
        return false;
      }
    },
    connect: async (id, silent = false) => {
      try {
        const a = await api.connectAccount(id);
        useLiveStore.getState().setLive(a);
        return true;
      } catch (e) {
        // Backend logs the failure too; the manual button toasts it
        // (auto-connect passes silent so a bad credential does not
        // pop a toast on every list load).
        if (!silent) toast.error(errText(e));
        return false;
      }
    },
    disconnect: async (id) => {
      try {
        await api.disconnectAccount(id);
      } catch (e) {
        toast.error(errText(e));
        useLiveStore.getState().removeLive(id);
        return false;
      }
      useLiveStore.getState().removeLive(id);
      return true;
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
