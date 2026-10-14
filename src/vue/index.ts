import { getCurrentInstance, getCurrentScope, onMounted, onScopeDispose, readonly, ref, toValue, watch, type MaybeRefOrGetter, type Ref } from 'vue';
import type { ConnectionStatus, RealtimeClient } from '../client';

/** Reactive connection status. Works in Vue 3 and Nuxt 3 (call from client-side code). */
export function useConnectionStatus(client: RealtimeClient): Readonly<Ref<ConnectionStatus>> {
  const status = ref<ConnectionStatus>(client.status);
  const off = client.onStatus((s) => { status.value = s; });
  if (getCurrentScope()) onScopeDispose(off);
  return readonly(status) as Readonly<Ref<ConnectionStatus>>;
}

/**
 * Subscribe to a channel for the lifetime of the component/scope.
 * `channel` may be a string, ref or getter; changing it re-subscribes.
 * In Nuxt, use inside `onMounted` or a `.client.ts` plugin — never on the server.
 */
export function useChannel<T = unknown>(
  client: RealtimeClient,
  channel: MaybeRefOrGetter<string | null | undefined>,
  handler: (data: T, channel: string) => void,
): void {
  const start = () => {
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
  };
  // Inside a component, wait for mount: onMounted never runs during SSR (no server sockets) and the first
  // client render then matches the SSR HTML (no hydration mismatch).
  const inst = getCurrentInstance();
  if (inst && !inst.isMounted) onMounted(start);
  else start();
}
