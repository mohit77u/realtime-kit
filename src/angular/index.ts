import { Observable } from 'rxjs';
import type { ConnectionStatus, DirectMessage, PresenceEvent, RealtimeClient, RoomMessage } from '../client';

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

/**
 * Messages in a room. Subscribing joins the room, unsubscribing leaves it.
 * A failed join (e.g. "forbidden") errors the stream.
 *
 *   messages$ = room$<Chat>(this.rt, 'room:7');
 */
export function room$<T = unknown>(client: RealtimeClient, room: string): Observable<RoomMessage<T>> {
  return new Observable<RoomMessage<T>>((subscriber) => {
    client.connect();
    const r = client.room<T>(room);
    const off = r.onMessage((m) => subscriber.next(m));
    r.join().catch((err) => subscriber.error(err));
    return () => { off(); void r.leave(); };
  });
}

/** Live member list of a room (user ids). Joins the room while subscribed. */
export function members$(client: RealtimeClient, room: string): Observable<string[]> {
  return new Observable<string[]>((subscriber) => {
    client.connect();
    const r = client.room(room);
    const off = r.onMembers((list) => subscriber.next(list));
    r.join().then((list) => subscriber.next(list), (err) => subscriber.error(err));
    return () => { off(); void r.leave(); };
  });
}

/** Presence events (someone came online / went offline) in a room. Joins the room while subscribed. */
export function presence$(client: RealtimeClient, room: string): Observable<PresenceEvent> {
  return new Observable<PresenceEvent>((subscriber) => {
    client.connect();
    const r = client.room(room);
    const off = r.onPresence((e) => subscriber.next(e));
    r.join().catch((err) => subscriber.error(err));
    return () => { off(); void r.leave(); };
  });
}

/** Direct messages and per-user notifications. */
export function direct$<T = unknown>(client: RealtimeClient): Observable<DirectMessage<T>> {
  return new Observable<DirectMessage<T>>((subscriber) => {
    client.connect();
    return client.onDirect<T>((m) => subscriber.next(m));
  });
}
