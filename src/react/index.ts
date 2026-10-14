import { useEffect, useRef, useSyncExternalStore } from 'react';
import type { ConnectionStatus, RealtimeClient } from '../client';

/** Current connection status of a client; re-renders on change. */
export function useConnectionStatus(client: RealtimeClient): ConnectionStatus {
  return useSyncExternalStore(
    (cb) => client.onStatus(cb),
    () => client.status,
    () => 'idle' as ConnectionStatus, // SSR snapshot: never connected on the server
  );
}

/** Subscribe to a channel for the lifetime of the component. Handler may change freely. */
export function useChannel<T = unknown>(
  client: RealtimeClient,
  channel: string | null | undefined,
  handler: (data: T, channel: string) => void,
): void {
  const ref = useRef(handler);
  ref.current = handler;
  useEffect(() => {
    if (!channel) return;
    client.connect(); // no-op if already connected; needed when created with autoConnect:false (SSR)
    return client.subscribe<T>(channel, (d, c) => ref.current(d, c));
  }, [client, channel]);
}
