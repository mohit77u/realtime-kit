import http from 'node:http';
import express from 'express';
import { Redis } from 'ioredis';
import { createRealtimeServer, RealtimeError } from 'realtime-kit/server';
import { createRedisAdapter } from 'realtime-kit/redis';

const PORT = Number(process.env.PORT || 3000);
const REDIS_URL = process.env.REDIS_URL || 'redis://127.0.0.1:6379';
const NS = process.env.NS || 'demo';

const db = new Redis(REDIS_URL); // YOUR data store (messages, todos). realtime-kit never stores anything.
const app = express();
app.use(express.static('dist'));
const httpServer = http.createServer(app);

const dmKey = (a, b) => `${NS}:dm:${[a, b].sort().join(':')}`;

const rt = createRealtimeServer({
  server: httpServer,
  adapter: createRedisAdapter({ url: REDIS_URL, channel: `${NS}:rk` }), // fan-out between instances

  // 1. Who is connecting?  token "demo-<name>" -> user { id: <name> }
  authenticate: (_req, token) => (token?.startsWith('demo-') ? { id: token.slice(5) } : null),
  userId: (user) => user.id,

  // 2. Rooms: public-* for everyone, private-<me> only for me
  rooms: {
    canJoin: (ctx, room) => room.startsWith('public-') || room === `private-${ctx.userId}`,
    onMessage: async (ctx, room, data) => {
      const text = String(data?.text ?? '').trim().slice(0, 500);
      if (!text) throw new RealtimeError('bad_request', 'empty message');
      const msg = { text, from: ctx.userId, at: Date.now() };
      await db.rpush(`${NS}:room:${room}`, JSON.stringify(msg)); // persist
      return msg; // this is what gets broadcast
    },
    history: async (_ctx, room, { limit }) =>
      (await db.lrange(`${NS}:room:${room}`, -limit, -1)).map((s) => JSON.parse(s)),
  },

  // 3. Direct messages (no room): any user can message any user
  direct: {
    onMessage: async (ctx, to, data) => {
      const text = String(data?.text ?? '').trim().slice(0, 500);
      if (!text) throw new RealtimeError('bad_request', 'empty message');
      const msg = { text, from: ctx.userId, to, at: Date.now() };
      await db.rpush(dmKey(ctx.userId, to), JSON.stringify(msg));
      return msg;
    },
    history: async (ctx, withUser, { limit }) =>
      (await db.lrange(dmKey(ctx.userId, withUser), -limit, -1)).map((s) => JSON.parse(s)),
  },

  // 4. Plain topic channel: everyone may subscribe to "todos"; only the server publishes
  authorize: (_ctx, action, channel) => action === 'subscribe' && channel === 'todos',
});

// 5. "Update in one place, shows up everywhere": change data, then push to every subscriber
rt.onRequest('todo.list', async () => (await db.lrange(`${NS}:todos`, 0, -1)).map((s) => JSON.parse(s)));
rt.onRequest('todo.add', async ({ title }, ctx) => {
  const todo = { id: Date.now().toString(36), title: String(title ?? '').slice(0, 200), by: ctx.userId };
  if (!todo.title) throw new RealtimeError('bad_request', 'title required');
  await db.rpush(`${NS}:todos`, JSON.stringify(todo));
  rt.publish('todos', { type: 'added', todo });                       // all subscribers, on all instances
  rt.sendToUser(ctx.userId, { notice: `saved "${todo.title}"` });     // all tabs/devices of this user
  return todo;
});

rt.onConnection((ctx) => console.log(`[${PORT}] + ${ctx.userId}`));
rt.onDisconnect((ctx) => console.log(`[${PORT}] - ${ctx.userId}`));

httpServer.listen(PORT, () => console.log(`demo on http://localhost:${PORT}`));
process.on('SIGTERM', async () => { await rt.close(); process.exit(0); });
