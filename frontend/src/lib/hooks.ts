"use client";

import useSWR, { type SWRConfiguration } from "swr";
import { fetcher } from "@/lib/api";

/** Data hook: GET `path` (null = skip), cached and de-duplicated by SWR, optionally polled. */
export function useApi<T>(path: string | null, opts: { refreshMs?: number } & SWRConfiguration<T> = {}) {
  const { refreshMs, ...rest } = opts;
  return useSWR<T>(path, fetcher as (p: string) => Promise<T>, {
    refreshInterval: refreshMs,
    revalidateOnFocus: false,
    keepPreviousData: true,
    shouldRetryOnError: false,
    ...rest,
  });
}
