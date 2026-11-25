import { create } from "zustand";

export type ThemeName = "light" | "warm" | "dark";

const KEY = "desss.theme";

/** Read the persisted theme; default is dark (the look shipped so far). */
function readInitial(): ThemeName {
  try {
    const v = localStorage.getItem(KEY);
    if (v === "light" || v === "warm" || v === "dark") return v;
  } catch {
    // Private mode / storage unavailable: fall through to the default.
  }
  return "dark";
}

/** The single place that touches the DOM: the theme lives on
 * `document.documentElement.dataset.theme`, which tokens.css reads. */
function apply(t: ThemeName) {
  document.documentElement.dataset.theme = t;
}

type S = {
  theme: ThemeName;
  setTheme: (t: ThemeName) => void;
};

export const useThemeStore = create<S>((set) => {
  const initial = readInitial();
  apply(initial);
  return {
    theme: initial,
    setTheme: (theme) => {
      try {
        localStorage.setItem(KEY, theme);
      } catch {
        // Keep the in-memory theme even if persistence fails.
      }
      apply(theme);
      set({ theme });
    },
  };
});
