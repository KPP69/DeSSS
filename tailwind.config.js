/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      // DeSSS design tokens (dark quant-terminal theme).
      // Pages and components must reference these semantic names instead of
      // scattering raw hex values. Source of truth: src/styles/tokens.css.
      colors: {
        app: "#0B0E14", // page background
        panel: "#121722", // card background
        overlay: "#181F2E", // modal / popover background
        line: "#1E2636", // unified border
        primary: "#3B82F6",
        profit: "#10B981",
        loss: "#EF4444",
        warn: "#F59E0B",
        t1: "#F1F5F9", // primary text
        t2: "#94A3B8", // secondary text
        t3: "#64748B", // muted text
      },
      fontFamily: {
        sans: ["Inter", "system-ui", "-apple-system", "Segoe UI", "sans-serif"],
        mono: ["JetBrains Mono", "Consolas", "SF Mono", "monospace"],
      },
    },
  },
  plugins: [],
};
