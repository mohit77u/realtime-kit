import { Observable } from 'rxjs';
import type { ConnectionStatus, RealtimeClient } from '../client';

/**
 * Observable of messages on a channel. Subscribing joins the channel, unsubscribing leaves it.
 * Use with the `async` pipe or `takeUntilDestroyed()`. No @angular/* dependency needed.
 *
 *   readonly news$ = channel$<News>(this.rt, 'public:news');
 */
export function channel$<T = unknown>(client: RealtimeClient, channel: string): Observable<T> {
  return new Observable<T>((subscriber) => {
    client.connect();
    return client.subscribe<T>(channel, (data) => subscriber.next(data));
  });
}

/** Observable of connection status; emits the current value immediately. */
export function status$(client: RealtimeClient): Observable<ConnectionStatus> {
  return new Observable<ConnectionStatus>((subscriber) => {
    subscriber.next(client.status);
    return client.onStatus((s) => subscriber.next(s));
  });
}

/** Observable wrapper for request/response. Emits once, then completes. */
export function request$<TRes = unknown>(client: RealtimeClient, method: string, data?: unknown): Observable<TRes> {
  return new Observable<TRes>((subscriber) => {
    let active = true;
    client.request<TRes>(method, data).then(
      (v) => { if (active) { subscriber.next(v); subscriber.complete(); } },
      (e) => { if (active) subscriber.error(e); },
    );
    return () => { active = false; };
  });
}
