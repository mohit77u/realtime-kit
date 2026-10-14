import {
  getCurrentInstance,
  getCurrentScope,
  onMounted,
  onScopeDispose,
  readonly,
  ref,
  toValue,
  watch,
  type MaybeRefOrGetter,
  type Ref,
} from 'vue';
import type { ConnectionStatus, DirectMessage, HistoryOptions, RealtimeClient, RoomMessage } from '../client';

/** Reactive connection status. Works in Vue 3 and Nuxt 3. */
export function useConnectionStatus(client: RealtimeClient): Readonly<Ref<ConnectionStatus>> {
  const status = ref<ConnectionStatus>(client.status);
  const off = client.onStatus((s) => { status.value = s; });
  if (getCurrentScope()) onScopeDispose(off);
  return readonly(status) as Readonly<Ref<ConnectionStatus>>;
}

/**
 * Run `start` when it is safe to touch the network. Inside a component that is on mount:
 * onMounted never runs during SSR (no server sockets) and the first client render matches the
 * SSR HTML (no hydration mismatch). Outside a component it runs immediately.
 */
function whenMounted(start: () => void): void {
  const inst = getCurrentInstance();
  if (inst && !inst.isMounted) onMounted(start);
  else start();
}

/**
 * Subscribe to a channel for the lifetime of the component/scope.
 * `channel` may be a string, ref or getter; changing it re-subscribes.
 */
export function useChannel<T = unknown>(
  client: RealtimeClient,
  channel: MaybeRefOrGetter<string | null | undefined>,
  handler: (data: T, channel: string) => void,
): void {
  whenMounted(() => {
    const stop = watch(
      () => toValue(channel),
      (ch, _prev, onCleanup) => {
        if (!ch) return;
        client.connect();
        onCleanup(client.subscribe<T>(ch, handler));
      },
      { immediate: true, flush: 'sync' },
    );
    if (getCurrentScope()) onScopeDispose(stop);
  });
}

export interface UseRoomResult<T> {
  joined: Readonly<Ref<boolean>>;
  /** User ids currently in the room, kept live. */
  members: Readonly<Ref<string[]>>;
  /** Set when joining failed (e.g. code "forbidden"). */
  error: Readonly<Ref<Error | null>>;
  send: (data: T) => Promise<{ id: string; at: number }>;
  history: (options?: HistoryOptions) => Promise<RoomMessage<T>[]>;
}

/** Join a room while mounted; leave on unmount. `room` may be a string, ref or getter (null = stay out). */
export function useRoom<T = unknown>(
  client: RealtimeClient,
  room: MaybeRefOrGetter<string | null | undefined>,
  onMessage?: (message: RoomMessage<T>) => void,
): UseRoomResult<T> {
  const joined = ref(false);
  const members = ref<string[]>([]);
  const error = ref<Error | null>(null);

  whenMounted(() => {
    const stop = watch(
      () => toValue(room),
      (name, _prev, onCleanup) => {
        if (!name) return;
        client.connect();
        const r = client.room<T>(name);
        let active = true;
        error.value = null;
        const offMsg = onMessage ? r.onMessage(onMessage) : () => undefined;
        const offMembers = r.onMembers((list) => { if (active) members.value = list; });
        r.join().then(
          (list) => { if (active) { members.value = list; joined.value = true; } },
          (err) => { if (active) error.value = err; },
        );
        onCleanup(() => {
          active = false;
          offMsg();
          offMembers();
          joined.value = false;
          members.value = [];
          void r.leave();
        });
      },
      { immediate: true, flush: 'sync' },
    );
    if (getCurrentScope()) onScopeDispose(stop);
  });

  const current = () => {
    const name = toValue(room);
    if (!name) throw new Error('useRoom: no room selected');
    return client.room<T>(name);
  };
  return {
    joined: readonly(joined) as Readonly<Ref<boolean>>,
    members: readonly(members) as Readonly<Ref<string[]>>,
    error: readonly(error) as Readonly<Ref<Error | null>>,
    send: (data) => current().send(data),
    history: (options) => current().history(options),
  };
}

/** Receive direct messages and per-user notifications for the lifetime of the component/scope. */
export function useDirect<T = unknown>(client: RealtimeClient, handler: (message: DirectMessage<T>) => void): void {
  whenMounted(() => {
    client.connect();
    const off = client.onDirect<T>(handler);
    if (getCurrentScope()) onScopeDispose(off);
  });
}
