import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import {
  PORTAL_FILTER_DEFAULTS,
  PortalFilterState,
} from '../types/portalFilters';
import {
  arePortalFiltersEqual,
  buildPortalSearchString,
  normalizePortalFilters,
  parsePortalFilters,
} from '../utils/urlFilters';

interface UpdateOptions {
  flush?: boolean;
  replace?: boolean;
}

interface UseUrlSyncedFiltersOptions {
  debounceMs?: number;
}

interface UseUrlSyncedFiltersResult {
  filters: PortalFilterState;
  setFilters: (
    updater: PortalFilterState | ((prev: PortalFilterState) => PortalFilterState),
    options?: UpdateOptions
  ) => void;
  updateFilters: (
    partial: Partial<PortalFilterState>,
    options?: UpdateOptions
  ) => void;
  resetFilters: (options?: UpdateOptions) => void;
}

const DEFAULT_DEBOUNCE_MS = 250;

const isUpdaterFn = (
  value: PortalFilterState | ((prev: PortalFilterState) => PortalFilterState)
): value is (prev: PortalFilterState) => PortalFilterState => typeof value === 'function';

/**
 * Keeps portal filter state in lockstep with the browser URL.
 *
 * - Hydrates state from `location.search` on mount.
 * - Normalizes and canonicalizes values before committing.
 * - Preserves unrelated query parameters when writing.
 * - Debounces writes for noisier inputs (e.g. free-text search).
 */
export const useUrlSyncedFilters = (
  options: UseUrlSyncedFiltersOptions = {}
): UseUrlSyncedFiltersResult => {
  const { debounceMs = DEFAULT_DEBOUNCE_MS } = options;
  const location = useLocation();
  const navigate = useNavigate();
  const [filters, setFiltersState] = useState<PortalFilterState>(() =>
    parsePortalFilters(location.search)
  );

  const lastCommittedSearchRef = useRef<string>(location.search || '');
  const pendingTimeoutRef = useRef<number | null>(null);

  const clearPendingTimeout = useCallback(() => {
    if (typeof window === 'undefined') {
      return;
    }
    if (pendingTimeoutRef.current) {
      window.clearTimeout(pendingTimeoutRef.current);
      pendingTimeoutRef.current = null;
    }
  }, []);

  const commitSearch = useCallback(
    (nextFilters: PortalFilterState, replace: boolean) => {
      const targetSearch = buildPortalSearchString(nextFilters, location.search);
      if (targetSearch === (lastCommittedSearchRef.current || '')) {
        return;
      }
      lastCommittedSearchRef.current = targetSearch;
      navigate({ pathname: location.pathname, search: targetSearch }, { replace });
    },
    [navigate, location.pathname, location.search]
  );

  const scheduleCommit = useCallback(
    (nextFilters: PortalFilterState, { flush = false, replace = false }: UpdateOptions = {}) => {
      const normalized = normalizePortalFilters(nextFilters);
      if (typeof window === 'undefined') {
        commitSearch(normalized, replace);
        return;
      }
      clearPendingTimeout();
      const delay = flush ? 0 : Math.max(debounceMs, 0);
      pendingTimeoutRef.current = window.setTimeout(() => {
        commitSearch(normalized, replace);
        clearPendingTimeout();
      }, delay);
    },
    [clearPendingTimeout, commitSearch, debounceMs]
  );

  const applyUpdater = useCallback(
    (
      updater: PortalFilterState | ((prev: PortalFilterState) => PortalFilterState),
      options?: UpdateOptions
    ) => {
      setFiltersState((prev) => {
        const next = isUpdaterFn(updater) ? updater(prev) : updater;
        const normalized = normalizePortalFilters(next);
        if (arePortalFiltersEqual(prev, normalized)) {
          return prev;
        }
        scheduleCommit(normalized, options);
        return normalized;
      });
    },
    [scheduleCommit]
  );

  const updateFilters = useCallback(
    (partial: Partial<PortalFilterState>, options?: UpdateOptions) => {
      applyUpdater((prev) => ({ ...prev, ...partial }), options);
    },
    [applyUpdater]
  );

  const resetFilters = useCallback(
    (options?: UpdateOptions) => {
      applyUpdater(PORTAL_FILTER_DEFAULTS, { flush: true, replace: true, ...options });
    },
    [applyUpdater]
  );

  useEffect(() => () => {
    clearPendingTimeout();
  }, [clearPendingTimeout]);

  useEffect(() => {
    clearPendingTimeout();
    const canonicalFilters = parsePortalFilters(location.search);
    const canonicalSearch = buildPortalSearchString(canonicalFilters, location.search);
    setFiltersState((prev) =>
      arePortalFiltersEqual(prev, canonicalFilters) ? prev : canonicalFilters
    );
    if (canonicalSearch !== (location.search || '')) {
      lastCommittedSearchRef.current = canonicalSearch;
      navigate({ pathname: location.pathname, search: canonicalSearch }, { replace: true });
      return;
    }
    lastCommittedSearchRef.current = location.search || '';
  }, [clearPendingTimeout, location.pathname, location.search, navigate]);

  return useMemo(
    () => ({ filters, setFilters: applyUpdater, updateFilters, resetFilters }),
    [applyUpdater, filters, resetFilters, updateFilters]
  );
};

export default useUrlSyncedFilters;
