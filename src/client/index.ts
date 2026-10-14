import { AUTH_PROTOCOL_PREFIX, SUBPROTOCOL, type ClientFrame, type DirectFrame, type RoomFrame, type ServerFrame } from '../shared/protocol';

export type ConnectionStatus = 'idle' | 'connecting' | 'open' | 'reconnecting' | 'closed';

export interface RealtimeClientOptions {
  /** ws:// or wss:// URL of the server endpoint, e.g. "wss://api.example.com/realtime". */
  url: string;
  /**
   * Returns a token sent during the handshake (as a WebSocket subprotocol, never in the URL).
   * Called on every (re)connect so refreshed tokens are picked up. Omit if you auth via cookies.
   */
  auth?: () => string | undefined | Promise<string | undefined>;
  /** Auto-reconnect with exponential backoff + jitter. Default true. */
  reconnect?: boolean;
  /** Backoff bounds in ms. Default 500 .. 15000. */
  minDelayMs?: number;
  maxDelayMs?: number;
  /** Request timeout in ms. Default 10000. */
  requestTimeoutMs?: number;
  /** Custom WebSocket constructor (e.g. `ws` on Node < 22). Defaults to global WebSocket. */
  WebSocket?: typeof WebSocket;
  /** Connect immediately. Default true. Set false in SSR and call connect() on the client. */
  autoConnect?: boolean;
}

/** A message received in a room. `from` is the sender's user id as verified by the server (null = server code). */
export interface RoomMessage<T = unknown> {
  id: string;
  room: string;
  from: string | null;
  data: T;
  at: number;
}

/** Someone came online (joined) or went offline (left) in a room. */
export interface PresenceEvent {
  room: string;
  userId: string;
  online: boolean;
  at: number;
}

/** A direct message or per-user notification. `from` is verified by the server (null = server code). */
export interface DirectMessage<T = unknown> {
  id: string;
  from: string | null;
  to: string;
  data: T;
  at: number;
}

export interface HistoryOptions {
  /** Cursor for pagination; passed to your server `history` hook. */
  before?: string | number;
  /** 1..100, default 50. */
  limit?: number;
}

export interface Room<T = unknown> {
  readonly name: string;
  /** True while this client is a member (re-joined automatically after reconnects). */
  readonly joined: boolean;
  /** Join (server runs your `canJoin`). Resolves with the user ids currently in the room. Reference-counted. */
  join(): Promise<string[]>;
  /** Leave. The room is really left when every `join()` has been matched by a `leave()`. */
  leave(): Promise<void>;
  /** Send a message to everyone in the room (including yourself and your other tabs). */
  send(data: T): Promise<{ id: string; at: number }>;
  /** Older messages, from your server `rooms.history` hook. */
  history(options?: HistoryOptions): Promise<RoomMessage<T>[]>;
  /** User ids currently in the room. */
  members(): Promise<string[]>;
  /** New messages. Returns an unsubscribe function. */
  onMessage(handler: (message: RoomMessage<T>) => void): () => void;
  /** Users coming online / going offline in this room. Returns an unsubscribe function. */
  onPresence(handler: (event: PresenceEvent) => void): () => void;
  /** Full member list whenever it changes (join, reconnect, someone comes/goes). Returns an unsubscribe function. */
  onMembers(handler: (members: string[]) => void): () => void;
}

interface RoomState {
  refs: number;
  joined: boolean;
  members: string[];
  messageHandlers: Set<(m: RoomMessage<any>) => void>;
  presenceHandlers: Set<(e: PresenceEvent) => void>;
  memberHandlers: Set<(members: string[]) => void>;
  handle?: Room<any>;
}

type Handler = (data: any, channel: string) => void;
type Listener<T extends unknown[]> = (...args: T) => void;

export class RealtimeError extends Error {
  constructor(public code: string, message: string) {
    super(message);
  }
}

export class RealtimeClient {
  private ws: WebSocket | null = null;
  private _status: ConnectionStatus = 'idle';
  private attempts = 0;
  private timer: ReturnType<typeof setTimeout> | null = null;
  private manualClose = false;
  /** Bumped by every open() and close(); lets a pending open() notice it has been superseded. */
  private epoch = 0;
  private queue: string[] = [];
  private subs = new Map<string, Set<Handler>>();
  private pending = new Map<string, { resolve: (v: any) => void; reject: (e: Error) => void; timer: ReturnType<typeof setTimeout> }>();
  private statusListeners = new Set<Listener<[ConnectionStatus]>>();
  private errorListeners = new Set<Listener<[RealtimeError]>>();
  private rooms = new Map<string, RoomState>();
  private directHandlers = new Set<(m: DirectMessage<any>) => void>();
  private reqCounter = 0;
  private readonly opts: Required<Pick<RealtimeClientOptions, 'reconnect' | 'minDelayMs' | 'maxDelayMs' | 'requestTimeoutMs'>> & RealtimeClientOptions;

  constructor(options: RealtimeClientOptions) {
    this.opts = { reconnect: true, minDelayMs: 500, maxDelayMs: 15_000, requestTimeoutMs: 10_000, ...options };
    if (options.autoConnect !== false && typeof (options.WebSocket ?? globalThis.WebSocket) !== 'undefined') {
      this.connect();
    }
  }

  get status(): ConnectionStatus { return this._status; }

  connect(): void {
    if (this.ws || this._status === 'connecting' || this._status === 'reconnecting') return;
    this.manualClose = false;
    void this.open();
  }

  /** Close for good (no reconnect). Pending requests are rejected. */
  close(): void {
    this.manualClose = true;
    this.epoch++; // cancel any open() that is still waiting for `auth`
    if (this.timer) clearTimeout(this.timer);
    this.ws?.close(1000);
    this.ws = null;
    this.rejectAll(new RealtimeError('closed', 'client closed'));
    this.setStatus('closed');
  }

  /** Subscribe to a channel. Returns an unsubscribe function. Survives reconnects. */
  subscribe<T = unknown>(channel: string, handler: (data: T, channel: string) => void): () => void {
    let set = this.subs.get(channel);
    if (!set) {
      this.subs.set(channel, (set = new Set()));
      this.sendFrame({ type: 'subscribe', channel });
    }
    set.add(handler as Handler);
    return () => {
      const s = this.subs.get(channel);
      if (!s) return;
      s.delete(handler as Handler);
      if (s.size === 0) {
        this.subs.delete(channel);
        this.sendFrame({ type: 'unsubscribe', channel }, false);
      }
    };
  }

  /** Publish to a channel (server must authorize client publishing). */
  publish(channel: string, data?: unknown): void {
    this.sendFrame({ type: 'publish', channel, data });
  }

  /** Call a server handler registered with `server.onRequest(method, ...)`. */
  request<TRes = unknown>(method: string, data?: unknown): Promise<TRes> {
    const id = `r${++this.reqCounter}`;
    return new Promise<TRes>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new RealtimeError('timeout', `request "${method}" timed out`));
      }, this.opts.requestTimeoutMs);
      this.pending.set(id, { resolve, reject, timer });
      this.sendFrame({ type: 'request', id, method, data });
    });
  }

  onStatus(cb: (status: ConnectionStatus) => void): () => void {
    this.statusListeners.add(cb);
    return () => this.statusListeners.delete(cb);
  }

  onError(cb: (err: RealtimeError) => void): () => void {
    this.errorListeners.add(cb);
    return () => this.errorListeners.delete(cb);
  }

  /** Get a room handle (cached). Call `join()` to enter it. */
  room<T = unknown>(name: string): Room<T> {
    let st = this.rooms.get(name);
    if (!st) {
      st = { refs: 0, joined: false, members: [], messageHandlers: new Set(), presenceHandlers: new Set(), memberHandlers: new Set() };
      this.rooms.set(name, st);
    }
    if (st.handle) return st.handle as Room<T>;
    const state = st;
    st.handle = {
      name,
      get joined() { return state.refs > 0 && state.joined; },
      join: async () => {
        state.refs++;
        try {
          if (!state.joined) {
            const r = await this.request<{ members: string[] }>('rk.room.join', { room: name });
            state.members = r.members;
            state.joined = true;
            this.emitMembers(state);
          }
          return [...state.members];
        } catch (err) {
          state.refs--;
          throw err;
        }
      },
      leave: async () => {
        if (state.refs === 0) return;
        if (--state.refs > 0) return;
        state.joined = false;
        await this.request('rk.room.leave', { room: name }).catch(() => undefined);
      },
      send: (data) => this.request('rk.room.send', { room: name, data }),
      history: async (options) =>
        (await this.request<{ messages: RoomMessage<T>[] }>('rk.room.history', { room: name, ...options })).messages,
      members: async () => (await this.request<{ members: string[] }>('rk.room.members', { room: name })).members,
      onMessage: (handler) => {
        state.messageHandlers.add(handler);
        return () => state.messageHandlers.delete(handler);
      },
      onPresence: (handler) => {
        state.presenceHandlers.add(handler);
        return () => state.presenceHandlers.delete(handler);
      },
      onMembers: (handler) => {
        state.memberHandlers.add(handler);
        return () => state.memberHandlers.delete(handler);
      },
    };
    return st.handle as Room<T>;
  }

  /** Send a direct message to a user (all of their tabs/devices). You also receive an echo on your other tabs. */
  sendToUser(userId: string | number, data?: unknown): Promise<{ id: string; at: number; delivered: boolean }> {
    return this.request('rk.dm.send', { to: String(userId), data });
  }

  /** Receive direct messages and per-user notifications. Returns an unsubscribe function. */
  onDirect<T = unknown>(handler: (message: DirectMessage<T>) => void): () => void {
    this.directHandlers.add(handler as (m: DirectMessage<any>) => void);
    return () => this.directHandlers.delete(handler as (m: DirectMessage<any>) => void);
  }

  /** The conversation with one user, from your server `direct.history` hook. */
  async directHistory<T = unknown>(withUser: string | number, options?: HistoryOptions): Promise<DirectMessage<T>[]> {
    return (await this.request<{ messages: DirectMessage<T>[] }>('rk.dm.history', { with: String(withUser), ...options })).messages;
  }

  // ---- internals ----

  private async open(): Promise<void> {
    const epoch = ++this.epoch;
    this.setStatus(this.attempts === 0 ? 'connecting' : 'reconnecting');
    let token: string | undefined;
    try {
      token = await this.opts.auth?.();
    } catch {
      if (epoch !== this.epoch) return;
      return this.scheduleReconnect();
    }
    // close() or another connect() happened while we awaited `auth`: this attempt is stale.
    if (epoch !== this.epoch || this.manualClose) return;

    const WS = this.opts.WebSocket ?? globalThis.WebSocket;
    const protocols = token ? [SUBPROTOCOL, AUTH_PROTOCOL_PREFIX + token] : [SUBPROTOCOL];
    const ws = new WS(this.opts.url, protocols);
    this.ws = ws;

    ws.onmessage = (ev: MessageEvent) => {
      if (this.ws !== ws || typeof ev.data !== 'string') return;
      let frame: ServerFrame;
      try { frame = JSON.parse(ev.data); } catch { return; }
      this.handleFrame(frame);
    };
    ws.onclose = () => {
      if (this.ws !== ws) return;
      this.ws = null;
      for (const st of this.rooms.values()) st.joined = false;
      if (this.manualClose) return;
      this.rejectAll(new RealtimeError('disconnected', 'connection lost'));
      this.scheduleReconnect();
    };
    ws.onerror = () => { /* onclose follows; reconnect handled there */ };
  }

  private handleFrame(frame: ServerFrame): void {
    switch (frame.type) {
      case 'ready':
        this.attempts = 0;
        this.setStatus('open');
        // Re-subscribe, then flush anything queued while offline.
        for (const channel of this.subs.keys()) this.rawSend({ type: 'subscribe', channel });
        for (const msg of this.queue.splice(0)) this.ws?.send(msg);
        for (const [room, st] of this.rooms) {
          if (st.refs > 0) this.rejoin(room, st);
        }
        return;
      case 'room':
        return this.handleRoomFrame(frame);
      case 'direct':
        return this.handleDirectFrame(frame);
      case 'message':
        this.subs.get(frame.channel)?.forEach((h) => {
          try { h(frame.data, frame.channel); } catch (e) { console.error('[realtime-kit] handler error', e); }
        });
        return;
      case 'response': {
        const p = this.pending.get(frame.id);
        if (p) { clearTimeout(p.timer); this.pending.delete(frame.id); p.resolve(frame.data); }
        return;
      }
      case 'error': {
        const err = new RealtimeError(frame.code, frame.message);
        const p = frame.id ? this.pending.get(frame.id) : undefined;
        if (p && frame.id) { clearTimeout(p.timer); this.pending.delete(frame.id); p.reject(err); }
        else this.errorListeners.forEach((l) => l(err));
      }
    }
  }

  private rejoin(room: string, st: RoomState): void {
    this.request<{ members: string[] }>('rk.room.join', { room }).then(
      (r) => { st.members = r.members; st.joined = true; this.emitMembers(st); },
      (err) => this.errorListeners.forEach((l) => l(err instanceof RealtimeError ? err : new RealtimeError('internal', String(err)))),
    );
  }

  private handleRoomFrame(frame: RoomFrame): void {
    const st = this.rooms.get(frame.room);
    if (!st) return;
    if (frame.event === 'message') {
      const msg: RoomMessage = { id: frame.id, room: frame.room, from: frame.from, data: frame.data, at: frame.at };
      st.messageHandlers.forEach((h) => safeCall(h, msg));
      return;
    }
    const userId = frame.from;
    if (!userId) return;
    const online = frame.event === 'join';
    st.members = online ? (st.members.includes(userId) ? st.members : [...st.members, userId]) : st.members.filter((m) => m !== userId);
    const event: PresenceEvent = { room: frame.room, userId, online, at: frame.at };
    st.presenceHandlers.forEach((h) => safeCall(h, event));
    this.emitMembers(st);
  }

  private emitMembers(st: RoomState): void {
    const list = [...st.members];
    st.memberHandlers.forEach((h) => safeCall(h, list));
  }

  private handleDirectFrame(frame: DirectFrame): void {
    const msg: DirectMessage = { id: frame.id, from: frame.from, to: frame.to, data: frame.data, at: frame.at };
    this.directHandlers.forEach((h) => safeCall(h, msg));
  }

  private sendFrame(frame: ClientFrame, queueIfOffline = true): void {
    if (this._status === 'open' && this.ws?.readyState === 1) this.rawSend(frame);
    else if (queueIfOffline && frame.type !== 'subscribe') this.queue.push(JSON.stringify(frame));
    // 'subscribe' frames are never queued: they are replayed from `subs` on ready.
  }

  private rawSend(frame: ClientFrame): void {
    if (this.ws?.readyState === 1) this.ws.send(JSON.stringify(frame));
  }

  private scheduleReconnect(): void {
    if (!this.opts.reconnect || this.manualClose) return this.setStatus('closed');
    const { minDelayMs, maxDelayMs } = this.opts;
    const base = Math.min(maxDelayMs, minDelayMs * 2 ** this.attempts++);
    const delay = base / 2 + Math.random() * (base / 2);
    this.setStatus('reconnecting');
    this.timer = setTimeout(() => void this.open(), delay);
  }

  private rejectAll(err: Error): void {
    for (const p of this.pending.values()) { clearTimeout(p.timer); p.reject(err); }
    this.pending.clear();
  }

  private setStatus(s: ConnectionStatus): void {
    if (this._status === s) return;
    this._status = s;
    this.statusListeners.forEach((l) => l(s));
  }
}

function safeCall<A>(fn: (arg: A) => void, arg: A): void {
  try { fn(arg); } catch (e) { console.error('[realtime-kit] handler error', e); }
}

export function createRealtimeClient(options: RealtimeClientOptions): RealtimeClient {
  return new RealtimeClient(options);
}
