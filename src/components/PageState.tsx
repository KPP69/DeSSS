import type { ReactNode } from "react";
import Button from "./ui/Button";
import Spinner from "./ui/Spinner";
import { EmptyState } from "./ui";

/**
 * The three page states: loading / error / empty. First matching
 * state wins. `empty` renders children as the empty body; loading
 * shows a spinner; error shows a retry button when onRetry is given;
 * empty shows an optional action below the text.
 */
export default function PageState({
  loading,
  error,
  empty,
  loadingText,
  onRetry,
  action,
  children,
}: {
  loading?: boolean;
  error?: string | null;
  empty?: boolean;
  loadingText?: string;
  onRetry?: () => void;
  action?: ReactNode;
  children?: ReactNode;
}) {
  if (loading)
    return (
      <EmptyState>
        <span className="inline-flex items-center gap-2 text-ds-text-2">
          <Spinner />
          {loadingText ?? "加载中…"}
        </span>
      </EmptyState>
    );
  if (error)
    return (
      <EmptyState>
        <span className="block text-ds-down">{error}</span>
        {onRetry && (
          <span className="mt-3 block">
            <Button variant="secondary" size="sm" onClick={onRetry}>
              重试
            </Button>
          </span>
        )}
      </EmptyState>
    );
  if (empty)
    return (
      <EmptyState>
        {children}
        {action && <span className="mt-3 block">{action}</span>}
      </EmptyState>
    );
  return <>{children}</>;
}
