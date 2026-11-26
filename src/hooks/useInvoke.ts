import { useCallback, useEffect, useRef, useState } from "react";

export type InvokeResult<T> = {
  data: T | null;
  loading: boolean;
  error: string | null;
  refresh: () => void;
};

/**
 * One async load, one lifecycle (Phase 2, I11): mount + `deps`
 * change + `refresh()` all run the same fetcher. A late response
 * after unmount — or after the deps changed — is discarded, never
 * applied. `data` keeps the last good value across refreshes, so a
 * failed refresh shows the old data with the error set instead of
 * blanking the page; gate a full-page loading state on
 * `loading && data === null`.
 */
export function useInvoke<T>(fn: () => Promise<T>, deps: unknown[]): InvokeResult<T> {
  const [data, setData] = useState<T | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [tick, setTick] = useState(0);
  const fnRef = useRef(fn);
  fnRef.current = fn;
  const refresh = useCallback(() => setTick((t) => t + 1), []);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    fnRef
      .current()
      .then((d) => {
        if (!cancelled) setData(d);
      })
      .catch((e) => {
        if (!cancelled) setError(e instanceof Error ? e.message : String(e));
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tick, ...deps]);

  return { data, loading, error, refresh };
}
