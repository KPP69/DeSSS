import React from "react";
import Button from "./Button";
import Tooltip from "./Tooltip";

/**
 * RefreshButton (F5.1a): the one refresh control (spec §5) — ghost
 * + refresh icon + 「刷新」. Loading shows a spinner and disables
 * the button (Button's loading). The tooltip uses the §7 wording
 * for 刷新：重新读取当前数据（不承诺网络）.
 */
export default function RefreshButton({
  onClick,
  loading = false,
  disabled = false,
  size = "md",
}: {
  onClick: () => void;
  loading?: boolean;
  disabled?: boolean;
  /** md is the toolbar height (h-8); sm keeps the trading page's compact refresh. */
  size?: "sm" | "md";
}) {
  return (
    <Tooltip content="重新读取当前数据（不承诺网络）">
      <Button variant="ghost" size={size} onClick={onClick} loading={loading} disabled={disabled}>
        <span className="inline-flex items-center gap-1.5">
          <svg
            className="h-3.5 w-3.5"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
            aria-hidden="true"
          >
            <path d="M21 12a9 9 0 1 1-2.64-6.36" />
            <polyline points="21 3 21 9 15 9" />
          </svg>
          刷新
        </span>
      </Button>
    </Tooltip>
  );
}
