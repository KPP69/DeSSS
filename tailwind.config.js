/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      // DeSSS design tokens (dark quant-terminal theme).
      // Pages and components must reference these semantic names instead of
      // scattering raw hex values. Source of truth: src/styles/tokens.css.
      colors: {
        app: "#0D1117", // page background
        panel: "#131720", // card background
        overlay: "#171D29", // modal / popover background
        line: "rgba(255,255,255,0.06)", // unified border, barely visible
        primary: "#3D6FB4", // muted steady blue
        profit: "#10B981",
        loss: "#EF4444",
        warn: "#F59E0B",
        t1: "#E6EDF3", // primary text
        t2: "#8B949E", // secondary text
        t3: "#5C6574", // muted text
      },
      fontFamily: {
        sans: ["Inter", "system-ui", "-apple-system", "Segoe UI", "sans-serif"],
        mono: ["JetBrains Mono", "Consolas", "SF Mono", "monospace"],
      },
    },
  },
  plugins: [],
};
