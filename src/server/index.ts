import { createServer, type IncomingMessage, type Server as HttpServer } from 'node:http';
import type { Duplex } from 'node:stream';
import { WebSocketServer, WebSocket, type RawData } from 'ws';
import {
  AUTH_PROTOCOL_PREFIX,
  SUBPROTOCOL,
  parseClientFrame,
  type ErrorCode,
  type ServerFrame,
} from '../shared/protocol';

export type { ErrorCode, ServerFrame } from '../shared/protocol';

export interface ClientContext<User = unknown> {
  id: string;
  user: User;
  ip: string;
  request: IncomingMessage;
}

export type Action = 'subscribe' | 'publish';

export interface RealtimeServerOptions<User = unknown> {
  /** Attach to an existing http(s) server (Express, Nest, Next custom server...). */
  server?: HttpServer;
  /** Or let realtime-kit create its own http server on this port. */
  port?: number;
  /** URL path that accepts WebSocket upgrades. Default: "/realtime". */
  path?: string;
  /**
   * Allowed browser Origins. Requests with a different Origin are rejected (prevents
   * cross-site WebSocket hijacking). Use "*" to disable the check (not recommended).
   * Requests without an Origin header (non-browser clients) are allowed.
   * Default: [] -> only same-host origins are accepted.
   */
  allowedOrigins?: string[] | '*';
  /**
   * Verify the connecting client. Read cookies/headers from `request`, or the token the
   * client sent via `auth` (passed as the second argument). Return a user object, or
   * null/throw to reject with 401. If omitted, every client is accepted as anonymous.
   */
  authenticate?: (request: IncomingMessage, token: string | undefined) => User | null | Promise<User | null>;
  /**
   * Decide whether a client may subscribe to / publish on a channel.
   * Default: subscribe allowed, client publish DENIED (server publishes only).
   */
  authorize?: (ctx: ClientContext<User>, action: Action, channel: string) => boolean | Promise<boolean>;
  /** Max size of one incoming message in bytes. Default 64 KiB. */
  maxPayloadBytes?: number;
  /** Per-connection rate limit. Default 50 messages / 1000 ms. */
  rateLimit?: { messages: number; perMs: number };
  /** Max simultaneous connections per IP. Default 50. */
  maxConnectionsPerIp?: number;
  /** Max channels one client may subscribe to. Default 100. */
  maxSubscriptionsPerClient?: number;
  /** Ping interval; dead connections are terminated. Default 30000 ms. */
  heartbeatMs?: number;
  /** Drop clients whose outgoing buffer exceeds this many bytes (slow consumers). Default 1 MiB. */
  maxBufferedBytes?: number;
  /** Trust X-Forwarded-For for client IP (set only behind a trusted proxy). Default false. */
  trustProxy?: boolean;
}

type RequestHandler<User> = (data: unknown, ctx: ClientContext<User>) => unknown | Promise<unknown>;

interface Conn<User> {
  ws: WebSocket;
  ctx: ClientContext<User>;
  channels: Set<string>;
  alive: boolean;
  windowStart: number;
  windowCount: number;
}

const BRAND = Symbol.for('realtime-kit.error');

export class RealtimeError extends Error {
  readonly [BRAND] = true;
  constructor(public code: ErrorCode, message: string) {
    super(message);
  }
}

// Brand check (not instanceof) so errors still match when the class is bundled twice (CJS adapters).
const isRealtimeError = (e: unknown): e is RealtimeError => !!e && (e as any)[BRAND] === true;

export interface RealtimeServer<User = unknown> {
  /** Send data to everyone subscribed to a channel. Returns number of recipients. */
  publish(channel: string, data?: unknown, opts?: { except?: string }): number;
  /** Register an RPC-style handler clients call with `client.request(method, data)`. */
  onRequest(method: string, handler: RequestHandler<User>): void;
  /** Hook for new authenticated connections. */
  onConnection(cb: (ctx: ClientContext<User>) => void): void;
  /** Hook for closed connections. */
  onDisconnect(cb: (ctx: ClientContext<User>) => void): void;
  /** Number of live connections. */
  readonly size: number;
  /** Subscribe server-side code to nothing; use to kick a client. */
  disconnect(clientId: string, code?: number, reason?: string): void;
  /** Gracefully stop. */
  close(): Promise<void>;
}

export function createRealtimeServer<User = unknown>(
  options: RealtimeServerOptions<User> = {},
): RealtimeServer<User> {
  const {
    path = '/realtime',
    allowedOrigins = [],
    maxPayloadBytes = 64 * 1024,
    rateLimit = { messages: 50, perMs: 1000 },
    maxConnectionsPerIp = 50,
    maxSubscriptionsPerClient = 100,
    heartbeatMs = 30_000,
    maxBufferedBytes = 1024 * 1024,
    trustProxy = false,
  } = options;

  const ownsHttp = !options.server;
  const http = options.server ?? createServer((_, res) => { res.writeHead(404).end(); });
  const wss = new WebSocketServer({
    noServer: true,
    maxPayload: maxPayloadBytes,
    handleProtocols: (protocols) => (protocols.has(SUBPROTOCOL) ? SUBPROTOCOL : false),
  });

  const conns = new Map<string, Conn<User>>();
  const channels = new Map<string, Set<Conn<User>>>();
  const ipCounts = new Map<string, number>();
  const handlers = new Map<string, RequestHandler<User>>();
  const onConn: Array<(c: ClientContext<User>) => void> = [];
  const onDisc: Array<(c: ClientContext<User>) => void> = [];
  let nextId = 0;

  const originAllowed = (req: IncomingMessage): boolean => {
    const origin = req.headers.origin;
    if (allowedOrigins === '*' || !origin) return true;
    if (allowedOrigins.includes(origin)) return true;
    if (allowedOrigins.length === 0) {
      try { return new URL(origin).host === req.headers.host; } catch { return false; }
    }
    return false;
  };

  const clientIp = (req: IncomingMessage): string => {
    if (trustProxy) {
      const xff = req.headers['x-forwarded-for'];
      const first = (Array.isArray(xff) ? xff[0] : xff)?.split(',')[0]?.trim();
      if (first) return first;
    }
    return req.socket.remoteAddress ?? 'unknown';
  };

  const reject = (socket: Duplex, status: number, text: string) => {
    socket.write(`HTTP/1.1 ${status} ${text}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n`);
    socket.destroy();
  };

  const extractToken = (req: IncomingMessage): string | undefined => {
    const header = req.headers['sec-websocket-protocol'];
    if (!header) return undefined;
    for (const p of header.split(',').map((s) => s.trim())) {
      if (p.startsWith(AUTH_PROTOCOL_PREFIX)) return p.slice(AUTH_PROTOCOL_PREFIX.length);
    }
    return undefined;
  };

  const send = (conn: Conn<User>, frame: ServerFrame): boolean => {
    const { ws } = conn;
    if (ws.readyState !== WebSocket.OPEN) return false;
    if (ws.bufferedAmount > maxBufferedBytes) {
      ws.close(1013, 'slow consumer');
      return false;
    }
    ws.send(JSON.stringify(frame));
    return true;
  };

  const fail = (conn: Conn<User>, code: ErrorCode, message: string, id?: string) =>
    send(conn, { type: 'error', id, code, message });

  const unsubscribe = (conn: Conn<User>, channel: string) => {
    conn.channels.delete(channel);
    const set = channels.get(channel);
    if (set) {
      set.delete(conn);
      if (set.size === 0) channels.delete(channel);
    }
  };

  const allowed = async (conn: Conn<User>, action: Action, channel: string): Promise<boolean> => {
    if (options.authorize) {
      try { return await options.authorize(conn.ctx, action, channel); } catch { return false; }
    }
    return action === 'subscribe';
  };

  const handleMessage = async (conn: Conn<User>, raw: RawData, isBinary: boolean) => {
    if (isBinary) return void fail(conn, 'bad_request', 'binary frames are not supported');

    const now = Date.now();
    if (now - conn.windowStart >= rateLimit.perMs) {
      conn.windowStart = now;
      conn.windowCount = 0;
    }
    if (++conn.windowCount > rateLimit.messages) {
      fail(conn, 'rate_limited', 'too many messages');
      if (conn.windowCount > rateLimit.messages * 2) conn.ws.close(1008, 'rate limit');
      return;
    }

    const frame = parseClientFrame(raw.toString());
    if (!frame) return void fail(conn, 'bad_request', 'invalid frame');

    switch (frame.type) {
      case 'subscribe': {
        if (conn.channels.size >= maxSubscriptionsPerClient && !conn.channels.has(frame.channel)) {
          return void fail(conn, 'forbidden', 'subscription limit reached');
        }
        if (!(await allowed(conn, 'subscribe', frame.channel))) {
          return void fail(conn, 'forbidden', `cannot subscribe to ${frame.channel}`);
        }
        conn.channels.add(frame.channel);
        let set = channels.get(frame.channel);
        if (!set) channels.set(frame.channel, (set = new Set()));
        set.add(conn);
        return;
      }
      case 'unsubscribe':
        return unsubscribe(conn, frame.channel);
      case 'publish': {
        if (!(await allowed(conn, 'publish', frame.channel))) {
          return void fail(conn, 'forbidden', `cannot publish to ${frame.channel}`);
        }
        server.publish(frame.channel, frame.data, { except: conn.ctx.id });
        return;
      }
      case 'request': {
        const handler = handlers.get(frame.method);
        if (!handler) return void fail(conn, 'not_found', `unknown method ${frame.method}`, frame.id);
        try {
          const data = await handler(frame.data, conn.ctx);
          send(conn, { type: 'response', id: frame.id, data });
        } catch (err) {
          if (isRealtimeError(err)) fail(conn, err.code, err.message, frame.id);
          else fail(conn, 'internal', 'internal error', frame.id); // never leak internals
        }
      }
    }
  };

  const onUpgrade = async (req: IncomingMessage, socket: Duplex, head: Buffer) => {
    const url = new URL(req.url ?? '/', 'http://localhost');
    if (url.pathname !== path) return; // not ours; let other upgrade listeners handle it

    socket.on('error', () => socket.destroy());

    if (!originAllowed(req)) return reject(socket, 403, 'Forbidden');

    const ip = clientIp(req);
    if ((ipCounts.get(ip) ?? 0) >= maxConnectionsPerIp) return reject(socket, 429, 'Too Many Requests');

    let user: User | null = null as User | null;
    try {
      user = options.authenticate
        ? await options.authenticate(req, extractToken(req))
        : (null as unknown as User); // anonymous
      if (options.authenticate && user == null) return reject(socket, 401, 'Unauthorized');
    } catch {
      return reject(socket, 401, 'Unauthorized');
    }

    if (socket.destroyed) return;
    wss.handleUpgrade(req, socket, head, (ws) => {
      const id = `c${++nextId}_${Math.random().toString(36).slice(2, 10)}`;
      const ctx: ClientContext<User> = { id, user: user as User, ip, request: req };
      const conn: Conn<User> = { ws, ctx, channels: new Set(), alive: true, windowStart: Date.now(), windowCount: 0 };
      conns.set(id, conn);
      ipCounts.set(ip, (ipCounts.get(ip) ?? 0) + 1);

      ws.on('pong', () => { conn.alive = true; });
      ws.on('message', (raw, isBinary) => { void handleMessage(conn, raw, isBinary); });
      ws.on('error', () => ws.terminate());
      ws.on('close', () => {
        for (const ch of [...conn.channels]) unsubscribe(conn, ch);
        conns.delete(id);
        const left = (ipCounts.get(ip) ?? 1) - 1;
        if (left <= 0) ipCounts.delete(ip); else ipCounts.set(ip, left);
        for (const cb of onDisc) cb(ctx);
      });

      send(conn, { type: 'ready' });
      for (const cb of onConn) cb(ctx);
    });
  };

  http.on('upgrade', onUpgrade);

  const heartbeat = setInterval(() => {
    for (const conn of conns.values()) {
      if (!conn.alive) { conn.ws.terminate(); continue; }
      conn.alive = false;
      conn.ws.ping();
    }
  }, heartbeatMs);
  heartbeat.unref();

  if (ownsHttp) http.listen(options.port ?? 0);

  const server: RealtimeServer<User> = {
    publish(channel, data, opts) {
      const set = channels.get(channel);
      if (!set) return 0;
      const frame: ServerFrame = { type: 'message', channel, data };
      let n = 0;
      for (const conn of set) {
        if (opts?.except === conn.ctx.id) continue;
        if (send(conn, frame)) n++;
      }
      return n;
    },
    onRequest(method, handler) { handlers.set(method, handler); },
    onConnection(cb) { onConn.push(cb); },
    onDisconnect(cb) { onDisc.push(cb); },
    get size() { return conns.size; },
    disconnect(clientId, code = 1008, reason = 'disconnected') { conns.get(clientId)?.ws.close(code, reason); },
    async close() {
      clearInterval(heartbeat);
      http.off('upgrade', onUpgrade);
      for (const conn of conns.values()) conn.ws.close(1001, 'server shutting down');
      await new Promise<void>((resolve) => wss.close(() => resolve()));
      if (ownsHttp) await new Promise<void>((resolve) => http.close(() => resolve()));
    },
  };
  return server;
}
