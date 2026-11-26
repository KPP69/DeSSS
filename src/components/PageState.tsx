import type { ReactNode } from "react";
import { EmptyState } from "./ui";

/**
 * The three page states (Phase 2, I11): loading / error / empty.
 * Pages render their content as children and let this component own
 * the tri-state; the first matching state wins, in this order.
 * `empty` renders the children as the empty body (StrategyPage's
 * shell uses it); loading and error render their own text.
 */
export default function PageState({
  loading,
  error,
  empty,
  loadingText,
  children,
}: {
  loading?: boolean;
  error?: string | null;
  empty?: boolean;
  loadingText?: string;
  children?: ReactNode;
}) {
  if (loading) return <EmptyState>{loadingText ?? "加载中…"}</EmptyState>;
  if (error) return <EmptyState>{error}</EmptyState>;
  if (empty) return <EmptyState>{children}</EmptyState>;
  return <>{children}</>;
}
