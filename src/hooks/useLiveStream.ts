import { useEffect } from "react";
import { api, onAccountLive, onBook, onLog } from "../lib/tauri";
import { useConsoleStore } from "../store/useConsoleStore";
import { useLiveStore } from "../store/useLiveStore";

/**
 * The app's only event subscriptions (Phase 2, I11): market://book,
 * account://live and app://log, subscribed once at the App root.
 * Pages read the stores, they never subscribe. The `cancelled`
 * guard closes the leak App.tsx had: an unmount before a listen
 * promise resolved used to leave the listener alive forever (the
 * unlisten function was assigned after cleanup already ran).
 *
 * The runtime snapshot hydrates books, account states and recent
 * logs instantly on load instead of waiting for the next push.
 */
export function useLiveStream() {
  useEffect(() => {
    let cancelled = false;
    let unbook: (() => void) | undefined;
    let unlive: (() => void) | undefined;
    let unlog: (() => void) | undefined;
    void onBook((b) => useLiveStore.getState().setBook(b)).then((u) => {
      if (cancelled) u();
      else unbook = u;
    });
    void onAccountLive((a) => useLiveStore.getState().setLive(a)).then((u) => {
      if (cancelled) u();
      else unlive = u;
    });
    void onLog((l) => useConsoleStore.getState().addLog(l)).then((u) => {
      if (cancelled) u();
      else unlog = u;
    });
    api
      .getRuntimeSnapshot()
      .then((snap) => {
        if (cancelled) return;
        snap.live.forEach((a) => useLiveStore.getState().setLive(a));
        snap.books.forEach((b) => useLiveStore.getState().setBook(b));
        useConsoleStore.getState().addLogs(snap.logs ?? []);
      })
      .catch((e) => {
        // The snapshot only accelerates the first paint — a failure
        // must not toast or block the UI, just one console line.
        console.error(
          `启动快照加载失败：${e instanceof Error ? e.message : String(e)}`,
        );
      });
    return () => {
      cancelled = true;
      unbook?.();
      unlive?.();
      unlog?.();
    };
  }, []);
}
