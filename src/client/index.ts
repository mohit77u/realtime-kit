import { AUTH_PROTOCOL_PREFIX, SUBPROTOCOL, type ClientFrame, type ServerFrame } from '../shared/protocol';

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
  private queue: string[] = [];
  private subs = new Map<string, Set<Handler>>();
  private pending = new Map<string, { resolve: (v: any) => void; reject: (e: Error) => void; timer: ReturnType<typeof setTimeout> }>();
  private statusListeners = new Set<Listener<[ConnectionStatus]>>();
  private errorListeners = new Set<Listener<[RealtimeError]>>();
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

  // ---- internals ----

  private async open(): Promise<void> {
    this.setStatus(this.attempts === 0 ? 'connecting' : 'reconnecting');
    let token: string | undefined;
    try {
      token = await this.opts.auth?.();
    } catch {
      return this.scheduleReconnect();
    }
    if (this.manualClose) return;

    const WS = this.opts.WebSocket ?? globalThis.WebSocket;
    const protocols = token ? [SUBPROTOCOL, AUTH_PROTOCOL_PREFIX + token] : [SUBPROTOCOL];
    const ws = new WS(this.opts.url, protocols);
    this.ws = ws;

    ws.onmessage = (ev: MessageEvent) => {
      if (typeof ev.data !== 'string') return;
      let frame: ServerFrame;
      try { frame = JSON.parse(ev.data); } catch { return; }
      this.handleFrame(frame);
    };
    ws.onclose = () => {
      if (this.ws !== ws) return;
      this.ws = null;
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
        return;
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

export function createRealtimeClient(options: RealtimeClientOptions): RealtimeClient {
  return new RealtimeClient(options);
}
