import { create } from "zustand";

export type ThemeName = "warm" | "dark";

const KEY = "desss.theme";

/** Read the persisted theme; default is warm. A stored "light"
 * (removed in F1) maps to warm and is written back, so the old
 * value never resurfaces. */
function readInitial(): ThemeName {
  try {
    const v = localStorage.getItem(KEY);
    if (v === "warm" || v === "dark") return v;
    if (v === "light") {
      try {
        localStorage.setItem(KEY, "warm");
      } catch {
        // Keep the in-memory theme even if persistence fails.
      }
      return "warm";
    }
  } catch {
    // Private mode / storage unavailable: fall through to the default.
  }
  return "warm";
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
