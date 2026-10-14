import { useCallback, useEffect, useRef, useState, useSyncExternalStore } from 'react';
import type { ConnectionStatus, DirectMessage, HistoryOptions, RealtimeClient, RoomMessage } from '../client';

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

export interface UseRoomResult<T> {
  /** True once the server accepted the join (false while joining, after leaving, or if denied). */
  joined: boolean;
  /** User ids currently in the room, kept live. */
  members: string[];
  /** Set when joining failed (e.g. code "forbidden"). */
  error: Error | null;
  send: (data: T) => Promise<{ id: string; at: number }>;
  history: (options?: HistoryOptions) => Promise<RoomMessage<T>[]>;
}

/** Join a room while the component is mounted; leave on unmount. Pass null to stay out. */
export function useRoom<T = unknown>(
  client: RealtimeClient,
  room: string | null | undefined,
  onMessage?: (message: RoomMessage<T>) => void,
): UseRoomResult<T> {
  const ref = useRef(onMessage);
  ref.current = onMessage;
  const [members, setMembers] = useState<string[]>([]);
  const [joined, setJoined] = useState(false);
  const [error, setError] = useState<Error | null>(null);

  useEffect(() => {
    if (!room) return;
    client.connect();
    const r = client.room<T>(room);
    let active = true;
    setError(null);
    const offMsg = r.onMessage((m) => ref.current?.(m));
    const offMembers = r.onMembers((list) => active && setMembers(list));
    r.join().then(
      (list) => { if (active) { setMembers(list); setJoined(true); } },
      (err) => { if (active) setError(err); },
    );
    return () => {
      active = false;
      offMsg();
      offMembers();
      setJoined(false);
      setMembers([]);
      void r.leave();
    };
  }, [client, room]);

  const send = useCallback((data: T) => client.room<T>(room as string).send(data), [client, room]);
  const history = useCallback((options?: HistoryOptions) => client.room<T>(room as string).history(options), [client, room]);
  return { joined, members, error, send, history };
}

/** Receive direct messages and per-user notifications while mounted. */
export function useDirect<T = unknown>(client: RealtimeClient, handler: (message: DirectMessage<T>) => void): void {
  const ref = useRef(handler);
  ref.current = handler;
  useEffect(() => {
    client.connect();
    return client.onDirect<T>((m) => ref.current(m));
  }, [client]);
}
