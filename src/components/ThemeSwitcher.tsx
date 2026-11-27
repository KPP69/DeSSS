import React from "react";
import { useThemeStore, type ThemeName } from "../store/useThemeStore";

const OPTIONS: { value: ThemeName; label: string }[] = [
  { value: "warm", label: "暖色" },
  { value: "dark", label: "深色" },
];

/** Miniature of a theme: panel on top, page background below
 * (static preview colors from tokens.css, theme-independent). */
function Swatch({ theme }: { theme: ThemeName }) {
  return (
    <span className="w-5 h-4 shrink-0 rounded-[4px] overflow-hidden border border-ds-line flex flex-col">
      <span className={theme === "warm" ? "flex-1 sw-warm-panel" : "flex-1 sw-dark-panel"} />
      <span className={theme === "warm" ? "flex-1 sw-warm-bg" : "flex-1 sw-dark-bg"} />
    </span>
  );
}

/**
 * Two-way theme switcher (sidebar bottom; the System page reuses it).
 * Each option is a color swatch + name. The active option uses the
 * accent soft tint + accent text; inactive options stay neutral.
 * Switching applies instantly and persists to localStorage via
 * useThemeStore.
 */
export default function ThemeSwitcher() {
  const theme = useThemeStore((s) => s.theme);
  const setTheme = useThemeStore((s) => s.setTheme);
  return (
    <div className="flex flex-col gap-1" role="group" aria-label="外观主题">
      {OPTIONS.map((o) => (
        <button
          key={o.value}
          type="button"
          onClick={() => setTheme(o.value)}
          aria-pressed={theme === o.value}
          className={`h-9 flex items-center gap-2 px-2.5 rounded-lg text-[13px] transition-colors ${
            theme === o.value
              ? "bg-ds-accent-soft/15 text-ds-accent font-medium"
              : "text-ds-text-2 hover:text-ds-text-1 hover:bg-ds-hover"
          }`}
        >
          <Swatch theme={o.value} />
          {o.label}
        </button>
      ))}
    </div>
  );
}
