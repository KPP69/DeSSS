import React from "react";
import { useThemeStore, type ThemeName } from "../store/useThemeStore";

const OPTIONS: { value: ThemeName; label: string }[] = [
  { value: "light", label: "浅色" },
  { value: "warm", label: "暖色" },
  { value: "dark", label: "深色" },
];

/**
 * Compact three-way theme switcher (top bar + System page share it).
 * The active option uses the accent soft tint + accent text; inactive
 * options stay neutral. Switching applies instantly and persists to
 * localStorage via useThemeStore.
 */
export default function ThemeSwitcher() {
  const theme = useThemeStore((s) => s.theme);
  const setTheme = useThemeStore((s) => s.setTheme);
  return (
    <div
      className="flex items-center rounded-lg border border-ds-line bg-ds-panel-2 p-0.5"
      role="group"
      aria-label="外观主题"
    >
      {OPTIONS.map((o) => (
        <button
          key={o.value}
          type="button"
          onClick={() => setTheme(o.value)}
          aria-pressed={theme === o.value}
          className={`h-7 px-2.5 rounded-md text-xs transition-colors ${
            theme === o.value
              ? "bg-ds-accent-soft/15 text-ds-accent font-medium"
              : "text-ds-text-2 hover:text-ds-text-1"
          }`}
        >
          {o.label}
        </button>
      ))}
    </div>
  );
}
