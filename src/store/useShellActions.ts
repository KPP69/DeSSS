import type { ReactNode } from "react";
import { create } from "zustand";
import type { PageKey } from "./useAppStore";

/**
 * The page-to-toolbar channel (F1): the current page registers its
 * toolbar content (left actions + right status) in a useEffect and
 * unregisters on unmount. AppShell only renders what is registered —
 * no page state is lifted to App.
 */
export type ShellContent = { actions?: ReactNode; status?: ReactNode };

type S = {
  byPage: Partial<Record<PageKey, ShellContent>>;
  register: (page: PageKey, content: ShellContent) => void;
  unregister: (page: PageKey) => void;
};

export const useShellActions = create<S>((set) => ({
  byPage: {},
  register: (page, content) =>
    set((s) => ({ byPage: { ...s.byPage, [page]: content } })),
  unregister: (page) =>
    set((s) => {
      const byPage = { ...s.byPage };
      delete byPage[page];
      return { byPage };
    }),
}));
