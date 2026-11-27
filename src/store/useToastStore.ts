import { create } from "zustand";

/**
 * Toasts (F1): top-right, auto-dismiss after 3s, success / error.
 * A toast shows the message it is given verbatim — write-op failures
 * pass the backend's own error text, never a rewrite. At most 3 are
 * kept; a click dismisses early.
 */
export type ToastKind = "success" | "error";
export type ToastItem = { id: number; kind: ToastKind; message: string };

let nextId = 1;

type S = {
  toasts: ToastItem[];
  push: (kind: ToastKind, message: string) => void;
  dismiss: (id: number) => void;
};

export const useToastStore = create<S>((set, get) => ({
  toasts: [],
  push: (kind, message) => {
    const id = nextId++;
    set((s) => ({ toasts: [...s.toasts.slice(-2), { id, kind, message }] }));
    setTimeout(() => get().dismiss(id), 3000);
  },
  dismiss: (id) => set((s) => ({ toasts: s.toasts.filter((t) => t.id !== id) })),
}));

/** Call from anywhere (stores included): `toast.error(errText(e))`. */
export const toast = {
  success: (message: string) => useToastStore.getState().push("success", message),
  error: (message: string) => useToastStore.getState().push("error", message),
};
