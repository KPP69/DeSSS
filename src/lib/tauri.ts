import { invoke } from "@tauri-apps/api/core";
import { listen, type UnlistenFn } from "@tauri-apps/api/event";
import type { Tick, Balance, AppInfo, AccountView, CredentialMap } from "./types";

export const api = {
  getAppInfo: () => invoke<AppInfo>("get_app_info"),
  startMock: () => invoke<void>("start_mock"),
  stopMock: () => invoke<void>("stop_mock"),
  getSnapshot: () => invoke<{ ticks: Tick[]; balances: Balance[] }>("get_snapshot"),
  // Account center (phase 2). Credentials go to the Rust vault exactly once;
  // nothing secret is ever returned.
  listAccounts: () => invoke<AccountView[]>("list_accounts"),
  createAccount: (label: string, exchangeId: string, credentials: CredentialMap) =>
    invoke<AccountView>("create_account", { label, exchangeId, credentials }),
  updateAccount: (id: string, label: string, exchangeId: string, credentials: CredentialMap | null) =>
    invoke<AccountView>("update_account", { id, label, exchangeId, credentials }),
  deleteAccount: (id: string) => invoke<void>("delete_account", { id }),
  getVaultStatus: () => invoke<string | null>("get_vault_status"),
};

export function onTick(cb: (t: Tick) => void): Promise<UnlistenFn> {
  return listen<Tick>("market://tick", (e) => cb(e.payload));
}
export function onBalance(cb: (b: Balance) => void): Promise<UnlistenFn> {
  return listen<Balance>("account://balance", (e) => cb(e.payload));
}
