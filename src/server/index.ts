import { randomUUID } from 'node:crypto';
import { createServer, type IncomingMessage, type Server as HttpServer } from 'node:http';
import type { Duplex } from 'node:stream';
import { WebSocketServer, WebSocket, type RawData } from 'ws';
import {
  AUTH_PROTOCOL_PREFIX,
  SUBPROTOCOL,
  isValidChannel,
  parseClientFrame,
  type ClusterAdapter,
  type DirectFrame,
  type ErrorCode,
  type RoomFrame,
  type ServerFrame,
} from '../shared/protocol';

export type { ClusterAdapter, DirectFrame, ErrorCode, RoomFrame, ServerFrame } from '../shared/protocol';

export interface ClientContext<User = unknown> {
  id: string;
  user: User;
  /** Stable user identity from the `userId` option. Undefined for anonymous connections. */
  userId?: string;
  ip: string;
  request: IncomingMessage;
}

export type Action = 'subscribe' | 'publish';

/** Pagination query passed to your `history` hooks. */
export interface HistoryQuery {
  /** Opaque cursor the client passed (e.g. a message id or timestamp). Return messages older than it. */
  before?: string | number;
  /** 1..100, default 50. */
  limit: number;
}

export interface RoomsOptions<User = unknown> {
  /**
   * Decide whether a user may join a room. **Default: deny** (rooms are private unless you opt in).
   * Use `() => true` for public rooms, or check your DB for private ones.
   */
  canJoin?: (ctx: ClientContext<User>, room: string) => boolean | Promise<boolean>;
  /**
   * Runs before a message is broadcast. Persist it here. The returned value is what gets broadcast
   * (return `data` unchanged to pass through). Throw `RealtimeError` to reject the message.
   */
  onMessage?: (ctx: ClientContext<User>, room: string, data: unknown) => unknown | Promise<unknown>;
  /** Load older messages for `room.history()`. Without it, history requests return `not_found`. */
  history?: (ctx: ClientContext<User>, room: string, query: HistoryQuery) => unknown[] | Promise<unknown[]>;
}

export interface DirectOptions<User = unknown> {
  /** May `ctx` send a direct message to user `to`? Default: allowed. */
  canSend?: (ctx: ClientContext<User>, to: string) => boolean | Promise<boolean>;
  /** Runs before delivery. Persist here. The returned value is delivered. Throw `RealtimeError` to reject. */
  onMessage?: (ctx: ClientContext<User>, to: string, data: unknown) => unknown | Promise<unknown>;
  /** Load the conversation between `ctx` and `withUser`. Without it, history requests return `not_found`. */
  history?: (ctx: ClientContext<User>, withUser: string, query: HistoryQuery) => unknown[] | Promise<unknown[]>;
}

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
   * Returns the stable id of an authenticated user (e.g. `user => user.id`). Required for rooms,
   * direct messages and presence. All of one user's tabs/devices share this id.
   */
  userId?: (user: User) => string | number;
  /** Rooms (group chat, presence). Needs `userId`. */
  rooms?: RoomsOptions<User>;
  /** Direct (1-to-1) messages and per-user notifications. Needs `userId`. */
  direct?: DirectOptions<User>;
  /**
   * Decide whether a client may subscribe to / publish on a plain channel.
   * Default: subscribe allowed, client publish DENIED (server publishes only).
   */
  authorize?: (ctx: ClientContext<User>, action: Action, channel: string) => boolean | Promise<boolean>;
  /**
   * Connect several server instances (e.g. `createRedisAdapter(...)` from "realtime-kit/redis")
   * so publishes reach clients on every instance.
   */
  adapter?: ClusterAdapter;
  /** How often instances exchange membership snapshots when an `adapter` is set. Default 10000 ms. */
  clusterSyncMs?: number;
  /** Called for non-fatal internal errors (e.g. adapter failures). Default: console.error. */
  onError?: (error: unknown) => void;
  /** Max size of one incoming message in bytes. Default 64 KiB. */
  maxPayloadBytes?: number;
  /** Per-connection rate limit. Default 50 messages / 1000 ms. */
  rateLimit?: { messages: number; perMs: number };
  /** Max simultaneous connections per IP. Default 50. */
  maxConnectionsPerIp?: number;
  /** Max channels + rooms one client may join. Default 100. */
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
  rooms: Set<string>;
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

export interface RoomHandle {
  /** Broadcast a message from the server (`from` is null). Returns local recipients. */
  send(data?: unknown): number;
  /** User ids currently in the room (across all instances when an `adapter` is set). */
  members(): string[];
  readonly size: number;
}

export interface RealtimeServer<User = unknown> {
  /** Send data to everyone subscribed to a plain channel. Returns number of local recipients. */
  publish(channel: string, data?: unknown, opts?: { except?: string }): number;
  /** Send a direct message/notification to every connection (tab/device) of a user. Returns local deliveries. */
  sendToUser(userId: string | number, data?: unknown, opts?: { from?: string }): number;
  /** Server-side access to a room: broadcast from your own code (e.g. after a DB change), list members. */
  room(name: string): RoomHandle;
  /** Is the user connected (to any instance when an `adapter` is set)? */
  isOnline(userId: string | number): boolean;
  /** Ids of connected users (across all instances when an `adapter` is set). */
  onlineUsers(): string[];
  /** Register an RPC-style handler clients call with `client.request(method, data)`. Names starting with "rk." are reserved. */
  onRequest(method: string, handler: RequestHandler<User>): void;
  /** Hook for new authenticated connections. */
  onConnection(cb: (ctx: ClientContext<User>) => void): void;
  /** Hook for closed connections. */
  onDisconnect(cb: (ctx: ClientContext<User>) => void): void;
  /** Number of live connections on this instance. */
  readonly size: number;
  /** Close one connection. */
  disconnect(clientId: string, code?: number, reason?: string): void;
  /** Close all connections of a user (e.g. on logout or ban). */
  disconnectUser(userId: string | number, code?: number, reason?: string): void;
  /** Gracefully stop. */
  close(): Promise<void>;
}

type ClusterEvent =
  | { o: string; k: 'ch'; channel: string; data: unknown; except?: string }
  | { o: string; k: 'room'; frame: RoomFrame }
  | { o: string; k: 'dm'; frame: DirectFrame; users: string[] }
  | { o: string; k: 'presence'; room: string; userId: string; online: boolean }
  | { o: string; k: 'user'; userId: string; online: boolean }
  | { o: string; k: 'hello' }
  | { o: string; k: 'state'; rooms: Record<string, string[]>; users: string[] }
  | { o: string; k: 'bye' };

/** What this instance knows about another instance's users (for cluster-wide presence). */
interface Remote {
  seen: number;
  rooms: Map<string, Set<string>>;
  users: Set<string>;
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
    clusterSyncMs = 10_000,
  } = options;

  const ownsHttp = !options.server;
  const http = options.server ?? createServer((_, res) => { res.writeHead(404).end(); });
  const wss = new WebSocketServer({
    noServer: true,
    maxPayload: maxPayloadBytes,
    handleProtocols: (protocols) => (protocols.has(SUBPROTOCOL) ? SUBPROTOCOL : false),
  });

  const instanceId = randomUUID();
  const conns = new Map<string, Conn<User>>();
  const channels = new Map<string, Set<Conn<User>>>();
  const userConns = new Map<string, Set<Conn<User>>>();
  const roomMembers = new Map<string, Map<string, Set<Conn<User>>>>(); // room -> userId -> connections
  const ipCounts = new Map<string, number>();
  const handlers = new Map<string, RequestHandler<User>>();
  const onConn: Array<(c: ClientContext<User>) => void> = [];
  const onDisc: Array<(c: ClientContext<User>) => void> = [];
  let nextId = 0;

  const reportError = (err: unknown) => (options.onError ?? ((e) => console.error('[realtime-kit]', e)))(err);

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

  // ---------- local delivery ----------

  const deliverChannel = (channel: string, data: unknown, except?: string): number => {
    const set = channels.get(channel);
    if (!set) return 0;
    const frame: ServerFrame = { type: 'message', channel, data };
    let n = 0;
    for (const conn of set) {
      if (except === conn.ctx.id) continue;
      if (send(conn, frame)) n++;
    }
    return n;
  };

  const deliverRoom = (room: string, frame: RoomFrame): number => {
    const members = roomMembers.get(room);
    if (!members) return 0;
    let n = 0;
    for (const set of members.values()) for (const conn of set) if (send(conn, frame)) n++;
    return n;
  };

  const deliverUsers = (users: string[], frame: DirectFrame): number => {
    let n = 0;
    for (const uid of users) {
      const set = userConns.get(uid);
      if (set) for (const conn of set) if (send(conn, frame)) n++;
    }
    return n;
  };

  // ---------- cluster fan-out ----------

  const cluster = (event: ClusterEvent) => {
    if (!options.adapter) return;
    try {
      void Promise.resolve(options.adapter.publish(JSON.stringify(event))).catch(reportError);
    } catch (err) {
      reportError(err);
    }
  };

  const publishChannel = (channel: string, data: unknown, except?: string): number => {
    cluster({ o: instanceId, k: 'ch', channel, data, except });
    return deliverChannel(channel, data, except);
  };

  const publishRoom = (frame: RoomFrame): number => {
    cluster({ o: instanceId, k: 'room', frame });
    return deliverRoom(frame.room, frame);
  };

  const publishDirect = (frame: DirectFrame, users: string[]): number => {
    cluster({ o: instanceId, k: 'dm', frame, users });
    return deliverUsers(users, frame);
  };

  const unsubscribe = (conn: Conn<User>, channel: string) => {
    conn.channels.delete(channel);
    const set = channels.get(channel);
    if (set) {
      set.delete(conn);
      if (set.size === 0) channels.delete(channel);
    }
  };

  // ---------- rooms ----------

  const roomEvent = (room: string, event: RoomFrame['event'], from: string | null, data?: unknown): RoomFrame => ({
    type: 'room', room, event, id: randomUUID(), from, data, at: Date.now(),
  });

  // Cluster-wide presence: a user is "in" a room if any connection on any instance is in it.
  const remote = new Map<string, Remote>();

  const presentIn = (room: string, uid: string): boolean => {
    if (roomMembers.get(room)?.has(uid)) return true;
    for (const r of remote.values()) if (r.rooms.get(room)?.has(uid)) return true;
    return false;
  };

  /** Apply `mutate`, then tell local room members about users whose cluster-wide presence changed. */
  const changePresence = (pairs: Array<[string, string]>, mutate: () => void) => {
    const before = pairs.map(([room, uid]) => presentIn(room, uid));
    mutate();
    pairs.forEach(([room, uid], i) => {
      const now = presentIn(room, uid);
      if (now !== before[i]) deliverRoom(room, roomEvent(room, now ? 'join' : 'leave', uid));
    });
  };

  const roomUsers = (room: string): string[] => {
    const all = new Set(roomMembers.get(room)?.keys());
    for (const r of remote.values()) r.rooms.get(room)?.forEach((u) => all.add(u));
    return [...all];
  };

  const remotePairs = (r: Remote): Array<[string, string]> =>
    [...r.rooms].flatMap(([room, users]) => [...users].map((u): [string, string] => [room, u]));

  const snapshot = () => ({
    rooms: Object.fromEntries([...roomMembers].map(([room, m]) => [room, [...m.keys()]])),
    users: [...userConns.keys()],
  });

  const dropRemote = (o: string) => {
    const r = remote.get(o);
    if (r) changePresence(remotePairs(r), () => remote.delete(o));
  };

  const remoteFor = (o: string): Remote => {
    let r = remote.get(o);
    if (!r) remote.set(o, (r = { seen: Date.now(), rooms: new Map(), users: new Set() }));
    r.seen = Date.now();
    return r;
  };

  const joinRoom = (conn: Conn<User>, room: string) => {
    const uid = conn.ctx.userId!;
    let firstLocal = false;
    changePresence([[room, uid]], () => {
      let members = roomMembers.get(room);
      if (!members) roomMembers.set(room, (members = new Map()));
      let set = members.get(uid);
      if (!set) { members.set(uid, (set = new Set())); firstLocal = true; }
      set.add(conn);
      conn.rooms.add(room);
    });
    if (firstLocal) cluster({ o: instanceId, k: 'presence', room, userId: uid, online: true });
  };

  const leaveRoom = (conn: Conn<User>, room: string) => {
    if (!conn.rooms.delete(room)) return;
    const uid = conn.ctx.userId!;
    const members = roomMembers.get(room);
    const set = members?.get(uid);
    if (!members || !set) return;
    let lastLocal = false;
    changePresence([[room, uid]], () => {
      set.delete(conn);
      if (set.size > 0) return; // user still has another tab/device in this room on this instance
      members.delete(uid);
      if (members.size === 0) roomMembers.delete(room);
      lastLocal = true;
    });
    if (lastLocal) cluster({ o: instanceId, k: 'presence', room, userId: uid, online: false });
  };

  // ---------- built-in requests (rk.*) ----------

  const needUser = (conn: Conn<User>): string => {
    if (!conn.ctx.userId) throw new RealtimeError('forbidden', 'identity required: set the "userId" server option and authenticate');
    return conn.ctx.userId;
  };
  const roomName = (v: unknown): string => {
    if (!isValidChannel(v)) throw new RealtimeError('bad_request', 'invalid room name');
    return v;
  };
  const needMember = (conn: Conn<User>, room: string) => {
    if (!conn.rooms.has(room)) throw new RealtimeError('forbidden', `join ${room} first`);
  };
  const historyQuery = (d: any): HistoryQuery => ({
    before: typeof d?.before === 'string' || typeof d?.before === 'number' ? d.before : undefined,
    limit: Math.min(Math.max(Math.floor(Number(d?.limit)) || 50, 1), 100),
  });
  const safe = async (fn: () => boolean | Promise<boolean>): Promise<boolean> => {
    try { return await fn(); } catch { return false; }
  };

  const internal = new Map<string, (data: any, conn: Conn<User>) => unknown | Promise<unknown>>([
    ['rk.room.join', async (d, conn) => {
      needUser(conn);
      const room = roomName(d?.room);
      if (!conn.rooms.has(room)) {
        const canJoin = options.rooms?.canJoin;
        if (!canJoin || !(await safe(() => canJoin(conn.ctx, room)))) {
          throw new RealtimeError('forbidden', `cannot join ${room}`);
        }
        if (conn.rooms.size + conn.channels.size >= maxSubscriptionsPerClient) {
          throw new RealtimeError('forbidden', 'subscription limit reached');
        }
        if (conn.ws.readyState !== WebSocket.OPEN) return undefined; // closed while awaiting canJoin
        joinRoom(conn, room);
      }
      return { members: roomUsers(room) };
    }],
    ['rk.room.leave', (d, conn) => { leaveRoom(conn, roomName(d?.room)); return { ok: true }; }],
    ['rk.room.members', (d, conn) => {
      const room = roomName(d?.room);
      needMember(conn, room);
      return { members: roomUsers(room) };
    }],
    ['rk.room.send', async (d, conn) => {
      const uid = needUser(conn);
      const room = roomName(d?.room);
      needMember(conn, room);
      const data = options.rooms?.onMessage ? await options.rooms.onMessage(conn.ctx, room, d?.data) : d?.data;
      needMember(conn, room); // may have left while the hook was running
      const frame = roomEvent(room, 'message', uid, data); // `from` is set by the server, never the client
      publishRoom(frame);
      return { id: frame.id, at: frame.at };
    }],
    ['rk.room.history', async (d, conn) => {
      const room = roomName(d?.room);
      needMember(conn, room);
      const history = options.rooms?.history;
      if (!history) throw new RealtimeError('not_found', 'room history is not configured');
      return { messages: await history(conn.ctx, room, historyQuery(d)) };
    }],
    ['rk.dm.send', async (d, conn) => {
      const from = needUser(conn);
      const to = d?.to;
      if (typeof to !== 'string' || to.length === 0 || to.length > 200) throw new RealtimeError('bad_request', 'invalid recipient');
      const canSend = options.direct?.canSend;
      if (canSend && !(await safe(() => canSend(conn.ctx, to)))) throw new RealtimeError('forbidden', 'cannot message this user');
      const data = options.direct?.onMessage ? await options.direct.onMessage(conn.ctx, to, d?.data) : d?.data;
      const frame: DirectFrame = { type: 'direct', id: randomUUID(), from, to, data, at: Date.now() };
      // Deliver to the recipient and echo to the sender's own tabs/devices.
      publishDirect(frame, to === from ? [to] : [to, from]);
      return { id: frame.id, at: frame.at, delivered: userConns.has(to) };
    }],
    ['rk.dm.history', async (d, conn) => {
      needUser(conn);
      const withUser = d?.with;
      if (typeof withUser !== 'string' || withUser.length === 0 || withUser.length > 200) throw new RealtimeError('bad_request', 'invalid user');
      const history = options.direct?.history;
      if (!history) throw new RealtimeError('not_found', 'direct history is not configured');
      return { messages: await history(conn.ctx, withUser, historyQuery(d)) };
    }],
  ]);

  // ---------- plain channels ----------

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
        if (conn.channels.size + conn.rooms.size >= maxSubscriptionsPerClient && !conn.channels.has(frame.channel)) {
          return void fail(conn, 'forbidden', 'subscription limit reached');
        }
        if (!(await allowed(conn, 'subscribe', frame.channel))) {
          return void fail(conn, 'forbidden', `cannot subscribe to ${frame.channel}`);
        }
        if (conn.ws.readyState !== WebSocket.OPEN) return;
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
        publishChannel(frame.channel, frame.data, conn.ctx.id);
        return;
      }
      case 'request': {
        try {
          const builtin = internal.get(frame.method);
          const handler = builtin ? undefined : handlers.get(frame.method);
          if (!builtin && !handler) return void fail(conn, 'not_found', `unknown method ${frame.method}`, frame.id);
          const data = builtin ? await builtin(frame.data, conn) : await handler!(frame.data, conn.ctx);
          send(conn, { type: 'response', id: frame.id, data });
        } catch (err) {
          if (isRealtimeError(err)) fail(conn, err.code, err.message, frame.id);
          else {
            reportError(err);
            fail(conn, 'internal', 'internal error', frame.id); // never leak internals
          }
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
    let userId: string | undefined;
    try {
      user = options.authenticate
        ? await options.authenticate(req, extractToken(req))
        : (null as unknown as User); // anonymous
      if (options.authenticate && user == null) return reject(socket, 401, 'Unauthorized');
      if (options.userId && user != null) {
        userId = String(options.userId(user));
        if (!userId || userId.length > 200) return reject(socket, 401, 'Unauthorized');
      }
    } catch {
      return reject(socket, 401, 'Unauthorized');
    }

    if (socket.destroyed) return;
    wss.handleUpgrade(req, socket, head, (ws) => {
      const id = `c${++nextId}_${Math.random().toString(36).slice(2, 10)}`;
      const ctx: ClientContext<User> = { id, user: user as User, userId, ip, request: req };
      const conn: Conn<User> = { ws, ctx, channels: new Set(), rooms: new Set(), alive: true, windowStart: Date.now(), windowCount: 0 };
      conns.set(id, conn);
      ipCounts.set(ip, (ipCounts.get(ip) ?? 0) + 1);
      if (userId) {
        let set = userConns.get(userId);
        if (!set) {
          userConns.set(userId, (set = new Set()));
          cluster({ o: instanceId, k: 'user', userId, online: true });
        }
        set.add(conn);
      }

      ws.on('pong', () => { conn.alive = true; });
      ws.on('message', (raw, isBinary) => { void handleMessage(conn, raw, isBinary); });
      ws.on('error', () => ws.terminate());
      ws.on('close', () => {
        for (const ch of [...conn.channels]) unsubscribe(conn, ch);
        for (const room of [...conn.rooms]) leaveRoom(conn, room);
        if (userId) {
          const set = userConns.get(userId);
          set?.delete(conn);
          if (set && set.size === 0) {
            userConns.delete(userId);
            cluster({ o: instanceId, k: 'user', userId, online: false });
          }
        }
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

  let syncTimer: ReturnType<typeof setInterval> | undefined;
  if (options.adapter) {
    const onClusterMessage = (message: string) => {
      let e: ClusterEvent;
      try { e = JSON.parse(message); } catch { return; }
      if (!e || typeof e.o !== 'string' || e.o === instanceId) return; // ignore our own events
      switch (e.k) {
        case 'ch': deliverChannel(e.channel, e.data, e.except); break;
        case 'room': deliverRoom(e.frame.room, e.frame); break;
        case 'dm': deliverUsers(e.users, e.frame); break;
        case 'presence': {
          const r = remoteFor(e.o);
          changePresence([[e.room, e.userId]], () => {
            const set = r.rooms.get(e.room) ?? new Set<string>();
            if (e.online) set.add(e.userId); else set.delete(e.userId);
            if (set.size > 0) r.rooms.set(e.room, set); else r.rooms.delete(e.room);
          });
          break;
        }
        case 'user': {
          const r = remoteFor(e.o);
          if (e.online) r.users.add(e.userId); else r.users.delete(e.userId);
          break;
        }
        case 'hello': cluster({ o: instanceId, k: 'state', ...snapshot() }); break; // a new instance wants our state
        case 'state': {
          const old = remote.get(e.o);
          const next: Remote = {
            seen: Date.now(),
            rooms: new Map(Object.entries(e.rooms ?? {}).map(([room, users]) => [room, new Set(users)])),
            users: new Set(e.users ?? []),
          };
          changePresence([...(old ? remotePairs(old) : []), ...remotePairs(next)], () => remote.set(e.o, next));
          break;
        }
        case 'bye': dropRemote(e.o); break;
      }
    };
    try {
      void Promise.resolve(options.adapter.subscribe(onClusterMessage))
        .then(() => cluster({ o: instanceId, k: 'hello' }))
        .catch(reportError);
    } catch (err) {
      reportError(err);
    }
    // Periodic full snapshot heals missed Pub/Sub events and doubles as a liveness heartbeat.
    syncTimer = setInterval(() => {
      cluster({ o: instanceId, k: 'state', ...snapshot() });
      const now = Date.now();
      for (const [o, r] of [...remote]) if (now - r.seen > clusterSyncMs * 3.5) dropRemote(o); // crashed instance
    }, clusterSyncMs);
    syncTimer.unref();
  }

  if (ownsHttp) http.listen(options.port ?? 0);

  const uidOf = (u: string | number) => String(u);

  const server: RealtimeServer<User> = {
    publish: (channel, data, opts) => publishChannel(channel, data, opts?.except),
    sendToUser(userId, data, opts) {
      const to = uidOf(userId);
      return publishDirect({ type: 'direct', id: randomUUID(), from: opts?.from ?? null, to, data, at: Date.now() }, [to]);
    },
    room: (name) => ({
      send: (data) => publishRoom(roomEvent(name, 'message', null, data)),
      members: () => roomUsers(name),
      get size() { return roomUsers(name).length; },
    }),
    isOnline: (userId) => {
      const id = uidOf(userId);
      if (userConns.has(id)) return true;
      for (const r of remote.values()) if (r.users.has(id)) return true;
      return false;
    },
    onlineUsers: () => {
      const all = new Set(userConns.keys());
      for (const r of remote.values()) r.users.forEach((u) => all.add(u));
      return [...all];
    },
    onRequest(method, handler) {
      if (method.startsWith('rk.')) throw new Error('method names starting with "rk." are reserved by realtime-kit');
      handlers.set(method, handler);
    },
    onConnection(cb) { onConn.push(cb); },
    onDisconnect(cb) { onDisc.push(cb); },
    get size() { return conns.size; },
    disconnect(clientId, code = 1008, reason = 'disconnected') { conns.get(clientId)?.ws.close(code, reason); },
    disconnectUser(userId, code = 1008, reason = 'disconnected') {
      for (const conn of [...(userConns.get(uidOf(userId)) ?? [])]) conn.ws.close(code, reason);
    },
    async close() {
      clearInterval(heartbeat);
      if (syncTimer) clearInterval(syncTimer);
      http.off('upgrade', onUpgrade);
      if (options.adapter) {
        try { await options.adapter.publish(JSON.stringify({ o: instanceId, k: 'bye' } satisfies ClusterEvent)); } catch { /* best effort */ }
      }
      for (const conn of conns.values()) conn.ws.close(1001, 'server shutting down');
      await new Promise<void>((resolve) => wss.close(() => resolve()));
      if (ownsHttp) await new Promise<void>((resolve) => http.close(() => resolve()));
      try { await options.adapter?.close(); } catch (err) { reportError(err); }
    },
  };
  return server;
}
