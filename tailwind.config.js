/** @type {import('tailwindcss').Config} */
// Every color is an RGB channel from src/styles/tokens.css, so the same
// class works in both themes (warm / dark) and at any alpha
// (`bg-ds-accent-soft/10`, `border-ds-line/50`). Components must use
// these semantic names, never raw hex.
const rgb = (name) => `rgb(var(--ds-${name}) / <alpha-value>)`;

export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        ds: {
          bg: rgb("bg"), // page background
          "bg-2": rgb("bg-2"), // hover / recessed background
          panel: rgb("panel"), // card background
          "panel-2": rgb("panel-2"), // modal / popover / input background
          line: rgb("line"), // unified border
          "line-strong": rgb("line-strong"), // emphasized border
          "text-1": rgb("text-1"), // primary text
          "text-2": rgb("text-2"), // secondary text
          "text-3": rgb("text-3"), // muted text
          accent: rgb("accent"), // primary action / selected state
          "accent-hover": rgb("accent-hover"),
          "accent-soft": rgb("accent-soft"), // soft tint, use at low alpha
          "accent-contrast": rgb("accent-contrast"), // text on filled accent
          up: rgb("up"), // profit / bid / long
          "up-hover": rgb("up-hover"),
          "up-soft": rgb("up-soft"),
          "up-contrast": rgb("up-contrast"),
          down: rgb("down"), // loss / ask / short
          "down-hover": rgb("down-hover"),
          "down-soft": rgb("down-soft"),
          "down-contrast": rgb("down-contrast"),
          warn: rgb("warn"),
          "warn-hover": rgb("warn-hover"),
          "warn-soft": rgb("warn-soft"),
          "warn-contrast": rgb("warn-contrast"),
          info: rgb("info"),
          "info-hover": rgb("info-hover"),
          "info-soft": rgb("info-soft"),
          "info-contrast": rgb("info-contrast"),
          overlay: rgb("overlay"), // modal scrim, use at /50–/60
          "focus-ring": rgb("focus-ring"), // 2px focus ring on controls
          hover: rgb("hover"), // row + ghost hover surface
          "hover-strong": rgb("hover-strong"), // secondary-button hover
          active: rgb("active"), // pressed surface
          "text-disabled": rgb("text-disabled"), // disabled foreground
          "bg-disabled": rgb("bg-disabled"), // disabled background
        },
      },
      fontFamily: {
        sans: ['"Segoe UI"', '"PingFang SC"', '"Microsoft YaHei"', "system-ui", "-apple-system", "sans-serif"],
        mono: ['"Cascadia Mono"', "Consolas", '"SF Mono"', "Menlo", "monospace"],
      },
      boxShadow: {
        card: "var(--ds-shadow-card)",
      },
    },
  },
  plugins: [],
};
