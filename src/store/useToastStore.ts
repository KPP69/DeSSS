import { create } from "zustand";

/**
 * Toasts (F1): below the title bar (F4.5), auto-dismiss after 3s,
 * success / error. Messages show verbatim — write-op failures pass
 * the backend's own error text, never a rewrite. At most 3 visible
 * at once; further pushes queue and are promoted (fresh 3s timer) as
 * visible ones dismiss.
 */
export type ToastKind = "success" | "error";
export type ToastItem = { id: number; kind: ToastKind; message: string };

let nextId = 1;

const AUTO_DISMISS_MS = 3000;

type S = {
  toasts: ToastItem[];
  queue: ToastItem[];
  push: (kind: ToastKind, message: string) => void;
  dismiss: (id: number) => void;
};

function schedule(id: number) {
  setTimeout(() => useToastStore.getState().dismiss(id), AUTO_DISMISS_MS);
}

export const useToastStore = create<S>((set, get) => ({
  toasts: [],
  queue: [],
  push: (kind, message) => {
    const item = { id: nextId++, kind, message };
    if (get().toasts.length < 3) {
      set((s) => ({ toasts: [...s.toasts, item] }));
      schedule(item.id);
    } else {
      set((s) => ({ queue: [...s.queue, item] }));
    }
  },
  dismiss: (id) => {
    const s = get();
    if (!s.toasts.some((t) => t.id === id)) return;
    const next = s.queue[0];
    set({
      toasts: s.toasts.filter((t) => t.id !== id),
      queue: s.queue.slice(1),
    });
    if (next) {
      set((s2) => ({ toasts: [...s2.toasts, next] }));
      schedule(next.id);
    }
  },
}));

/** Call from anywhere (stores included): `toast.error(errText(e))`. */
export const toast = {
  success: (message: string) => useToastStore.getState().push("success", message),
  error: (message: string) => useToastStore.getState().push("error", message),
};
