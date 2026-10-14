# realtime-kit

Lightweight, secure WebSocket toolkit for **chat rooms, direct messages, live updates, notifications and presence**. Works with Node, Express, NestJS, Next.js, React, Vue, Nuxt, Angular and plain JavaScript. Scales to several servers with Redis.

- **Everything chat-like is built in:** rooms, 1-to-1 messages, "who is online", message history hooks, notifications to a user (all their tabs/devices), plus plain topic channels and request/response.
- **Tiny:** browser client ~3 kB gzip, zero dependencies. Server needs only [`ws`](https://github.com/websockets/ws). Redis support is optional.
- **Secure by default:** origin check, handshake auth, rooms are private unless you allow them, the sender of every message is set by the server (cannot be spoofed), rate limits, payload limits.
- **Resilient client:** auto-reconnect with backoff, automatic re-subscribe and **automatic room re-join**, offline queue, request timeouts.
- **Your database stays yours:** realtime-kit delivers messages; you persist them in `onMessage` hooks and serve history from your own DB.

## Contents

1. [Install](#1-install)
2. [Core concepts](#2-core-concepts)
3. [Quick start (Express + React)](#3-quick-start-express--react)
4. [Server setup](#4-server-setup) – Node · Express · NestJS · Next.js custom server
5. [Client setup & framework helpers](#5-client-setup--framework-helpers) – Vanilla · React · Next.js · Vue · Nuxt · Angular
6. [Recipes](#6-recipes) – group chat · direct messages · notifications · "update here, see it everywhere" · typing · read receipts · live feed
7. [Authentication](#7-authentication)
8. [Authorization](#8-authorization)
9. [Scaling with Redis](#9-scaling-with-redis)
10. [Production deployment](#10-production-deployment)
11. [API reference](#11-api-reference)
12. [Security model](#12-security-model)
13. [Troubleshooting](#13-troubleshooting)
14. [Tested with & limitations](#14-tested-with--limitations)

---

## 1. Install

```bash
npm install realtime-kit
```

Install only what your stack needs. Framework packages are optional peer dependencies:

| You use | Also have installed | Import from |
|---|---|---|
| Node / Express / any Node server | – | `realtime-kit/server` |
| Any browser app or Node script | – | `realtime-kit/client` |
| React / Next.js | `react >= 17` | `realtime-kit/react` |
| Vue 3 / Nuxt 3 | `vue >= 3.3` | `realtime-kit/vue` |
| Angular | `rxjs >= 7` | `realtime-kit/angular` |
| NestJS | `@nestjs/common`, `@nestjs/core >= 9`, `reflect-metadata` | `realtime-kit/nest` |
| Several server instances | `ioredis >= 5` and a Redis server | `realtime-kit/redis` |

Requirements: Node 18+ on the server, any modern browser on the client.

> **Which entry do I import?** Backend code imports `realtime-kit/server` (or `/nest`, `/redis`). Frontend code imports `realtime-kit/client` plus your framework helper. **Never import `/server` or `/redis` in frontend code.**

---

## 2. Core concepts

```
 Browser / Node clients ──ws://──▶  Your HTTP server (Express / Nest / Next / plain Node)
   rt.room('r1').send(...)             createRealtimeServer({ server, ... })
   rt.sendToUser('bob', ...)           rt.room('r1').send(...)       (from your code)
   rt.subscribe('orders', ...)         rt.sendToUser('bob', ...)     (from your code)
   rt.request('getUser', ...)          rt.publish('orders', ...)     (from your code)
                                       rt.onRequest('getUser', ...)
```

| Concept | What it is | Use it for |
|---|---|---|
| **User** | Identified by `userId(user)` on the server. One user can have many connections (tabs, phones). | Everything below |
| **Room** | A named group. Members join, everyone in the room gets every message, joins/leaves are announced. **Private by default**: you decide who may join (`canJoin`). | Group chat, support tickets, game lobbies, per-document collaboration |
| **Direct message** | Message to one user (all their tabs/devices). The sender's other tabs get an echo. No room needed. | 1-to-1 chat, notifications, "your export is ready" |
| **Channel** | A plain topic: server publishes, subscribed clients receive. No identity needed. | Live dashboards, prices, "orders changed", feature flags |
| **Request** | `rt.request('method', data)` → server handler returns a result. | Load data, save a todo, mark as read |
| **Presence** | Who is currently in a room (cluster-wide) and join/leave events. | Online lists |

The protocol is JSON text frames. Binary frames are rejected.

> **What realtime-kit does not do:** store messages. Persist them in your own DB inside the `onMessage` hook, and serve old messages from the `history` hook.

---

## 3. Quick start (Express + React)

A complete working chat in two files. (A fuller example with history, DMs and Redis is in [`examples/express-react-chat`](./examples/express-react-chat).)

**Server** (`server.js`):

```js
import http from 'node:http';
import express from 'express';
import { createRealtimeServer } from 'realtime-kit/server';

const app = express();
const httpServer = http.createServer(app);

const rt = createRealtimeServer({
  server: httpServer,
  authenticate: (_req, token) => (token ? { id: token } : null),   // demo only: token = user id. Use JWT/cookies for real.
  userId: (user) => user.id,
  rooms: { canJoin: (_ctx, room) => room.startsWith('public-') },  // who may join which room
});

httpServer.listen(3000);
```

**Client** (React):

```tsx
import { useState } from 'react';
import { createRealtimeClient } from 'realtime-kit/client';
import { useRoom } from 'realtime-kit/react';

const rt = createRealtimeClient({ url: 'ws://localhost:3000/realtime', auth: () => 'ann', autoConnect: false });

export function Chat() {
  const [msgs, setMsgs] = useState<string[]>([]);
  const [text, setText] = useState('');
  const room = useRoom<{ text: string }>(rt, 'public-general', (m) => setMsgs((x) => [...x, `${m.from}: ${m.data.text}`]));

  return (
    <div>
      <p>online: {room.members.join(', ')}</p>
      <ul>{msgs.map((m, i) => <li key={i}>{m}</li>)}</ul>
      <input value={text} onChange={(e) => setText(e.target.value)} />
      <button onClick={() => room.send({ text }).then(() => setText(''))}>send</button>
    </div>
  );
}
```

Open two browsers with different `auth` values and they chat. Everything else in this README builds on this.

---

## 4. Server setup

The server **attaches to an existing HTTP server** (sharing your port, TLS and process) or creates its own.

### 4.1 Plain Node

```js
import http from 'node:http';
import { createRealtimeServer } from 'realtime-kit/server';

const httpServer = http.createServer((req, res) => res.end('ok'));
const rt = createRealtimeServer({ server: httpServer, allowedOrigins: ['https://app.example.com'] });
httpServer.listen(3000);
```

Without `server`, pass `port` and realtime-kit creates its own HTTP server: `createRealtimeServer({ port: 3000 })`. CommonJS works too: `const { createRealtimeServer } = require('realtime-kit/server')`.

### 4.2 Express

```js
import http from 'node:http';
import express from 'express';
import { createRealtimeServer, RealtimeError } from 'realtime-kit/server';

const app = express();
const httpServer = http.createServer(app);              // 1. wrap Express in an http server

const rt = createRealtimeServer({                        // 2. attach realtime-kit to that server
  server: httpServer,
  allowedOrigins: ['http://localhost:5173', 'https://app.example.com'],
  authenticate: (req, token) => verifyJwt(token),        //    return the user, or null to reject (401)
  userId: (user) => user.id,                             //    required for rooms, DMs and presence
  rooms:  { /* canJoin, onMessage, history */ },         //    see section 6
  direct: { /* canSend, onMessage, history */ },
});

rt.onRequest('whoami', (_data, ctx) => ctx.user);

app.post('/api/orders/:id/ship', async (req, res) => {   // 3. push from any normal route
  await db.ship(req.params.id);
  rt.publish('orders', { type: 'shipped', id: req.params.id });
  res.json({ ok: true });
});

httpServer.listen(3000);                                 // 4. listen on httpServer, NOT app.listen()
```

> Use `httpServer.listen()`. `app.listen()` creates a different server realtime-kit is not attached to.

### 4.3 NestJS (Express or Fastify)

Install: `npm i realtime-kit @nestjs/common @nestjs/core reflect-metadata`

**Step 1 – register the module** (`app.module.ts`):

```ts
import { Module } from '@nestjs/common';
import { RealtimeModule } from 'realtime-kit/nest';

@Module({
  imports: [
    RealtimeModule.forRoot({
      allowedOrigins: ['https://app.example.com'],
      authenticate: (req, token) => verifyJwt(token),
      userId: (user) => user.id,
      rooms: { canJoin: (ctx, room) => room.startsWith('public-') },
    }),
  ],
})
export class AppModule {}
```

Options from `ConfigService` or other async sources:

```ts
RealtimeModule.forRootAsync({
  imports: [ConfigModule],
  inject: [ConfigService],
  useFactory: (config: ConfigService) => ({
    allowedOrigins: config.get<string>('WS_ORIGINS')!.split(','),
    authenticate: (req, token) => jwt.verifyOrNull(token),
    userId: (user) => user.id,
  }),
});
```

**Step 2 – inject `RealtimeService`** anywhere (`orders.service.ts`):

```ts
import { Injectable, OnModuleInit } from '@nestjs/common';
import { RealtimeService, RealtimeError } from 'realtime-kit/nest';

@Injectable()
export class OrdersService implements OnModuleInit {
  constructor(private readonly rt: RealtimeService) {}

  onModuleInit() {
    // Safe here: registrations are queued until the HTTP server is ready.
    this.rt.onRequest('order.get', async ({ id }, ctx) => {
      const order = await this.find(id);
      if (order.userId !== ctx.userId) throw new RealtimeError('forbidden', 'not your order');
      return order;
    });
  }

  async ship(id: string) {
    const order = await this.markShipped(id);
    this.rt.sendToUser(order.userId, { notice: `Order ${id} shipped` });   // to all tabs/devices of that user
    this.rt.publish('orders', { type: 'shipped', id });                      // to everyone subscribed
    this.rt.room(`order-${id}`).send({ status: 'shipped' });                 // to a room
  }
}
```

`RealtimeService` has the same server methods as the plain API: `publish`, `sendToUser`, `room(name)`, `isOnline`, `onlineUsers`, `onRequest`, `onConnection`, `onDisconnect`, `disconnect`, `disconnectUser`, `size`.

**Step 3 – bootstrap** (`main.ts`), identical for Express and Fastify:

```ts
const app = await NestFactory.create(AppModule);                   // or: new FastifyAdapter() as the 2nd argument
app.enableShutdownHooks();                                         // closes sockets (and Redis) on SIGTERM
await app.listen(3000);
```

The endpoint is `ws://host:3000/realtime`.

### 4.4 Next.js (custom server)

Next.js route handlers and Vercel/serverless functions **cannot hold WebSocket connections**. To run WebSockets next to Next.js, use a **custom server** (self-hosted Node, Docker, VPS), or run realtime-kit as a separate service and connect to it from Next.

`server.js` (CommonJS):

```js
const http = require('node:http');
const next = require('next');
const { createRealtimeServer } = require('realtime-kit/server');

const dev = process.env.NODE_ENV !== 'production';
const app = next({ dev });
const handle = app.getRequestHandler();

app.prepare().then(() => {
  const server = http.createServer((req, res) => handle(req, res));

  const rt = createRealtimeServer({
    server,                                        // same server and port as Next.js
    authenticate: (req, token) => verifyToken(token),
    userId: (user) => user.id,
    rooms: { canJoin: (ctx, room) => room.startsWith('public-') },
  });
  rt.onRequest('whoami', (_d, ctx) => ctx.user);

  server.listen(3000);
});
```

Set scripts `"dev": "node server.js"` and `"start": "NODE_ENV=production node server.js"`. Next's own HMR socket keeps working in dev; realtime-kit only handles upgrades on its own `path`. Page and socket share one origin, so no `allowedOrigins` needed.

### 4.5 Server options for rooms, direct messages and presence

These three options turn on the chat features. All are optional; without `userId` only plain channels and requests work.

```ts
createRealtimeServer({
  server,
  authenticate: (req, token) => verifyJwt(token),   // who is this?   (return user or null)
  userId: (user) => user.id,                         // stable id; all tabs/devices of a user share it

  rooms: {
    canJoin:   (ctx, room) => isMember(ctx.userId, room),      // DEFAULT: deny. Return true to allow.
    onMessage: async (ctx, room, data) => {                      // runs before broadcast
      const msg = { text: String(data.text).slice(0, 2000), at: Date.now() };
      await db.messages.insert({ room, from: ctx.userId, ...msg });  // persist here
      return msg;                                                    // what you return is what is broadcast
    },                                                             // throw RealtimeError to reject
    history: (ctx, room, { before, limit }) =>                   // serves room.history()
      db.messages.find({ room, before, limit }),
  },

  direct: {
    canSend:   (ctx, toUserId) => !isBlocked(toUserId, ctx.userId),   // default: allowed
    onMessage: async (ctx, to, data) => { await db.dm.insert({ from: ctx.userId, to, data }); return data; },
    history:   (ctx, withUser, { before, limit }) => db.dm.between(ctx.userId, withUser, { before, limit }),
  },
});
```

Notes:
- `from` on every message is **set by the server** from the verified user. A client cannot fake it.
- `before` is an opaque cursor the client passes (message id, timestamp...). `limit` is clamped to 1–100.
- `canJoin` may be `async` (check your database).
- Clients send `data` as any JSON. Validate and trim it in `onMessage`.

---

## 5. Client setup & framework helpers

All clients share the same core:

```ts
import { createRealtimeClient } from 'realtime-kit/client';

const rt = createRealtimeClient({
  url: 'wss://api.example.com/realtime',   // ws:// for local dev, wss:// in production
  auth: () => getAccessToken(),            // optional; see Authentication
});

rt.onStatus((s) => console.log(s));        // 'idle' | 'connecting' | 'open' | 'reconnecting' | 'closed'
rt.onError((e) => console.warn(e.code, e.message));

// rooms
const room = rt.room('public-general');
await room.join();                          // resolves with the user ids in the room
room.onMessage((m) => console.log(m.from, m.data));
await room.send({ text: 'hi' });

// direct messages
rt.onDirect((m) => console.log(m.from, m.data));
await rt.sendToUser('bob', { text: 'hey' });

// topic channel + request/response
rt.subscribe('orders', (data) => console.log(data));
const me = await rt.request('whoami');
```

> **SSR rule (Next, Nuxt, Angular SSR):** frameworks that render on the server run your module/component code there too. Create the client with `autoConnect: false`. The framework helpers (`useRoom`, `useChannel`, `room$`...) connect for you in the browser.

### 5.1 Vanilla JS / any bundler

Use the snippet above. In a `<script type="module">` page served by a bundler (Vite, webpack, esbuild) it works as is.

### 5.2 Node scripts (client in Node)

Node 22+ has a global `WebSocket`. On older Node, pass the `ws` package:

```js
import WebSocket from 'ws';
const rt = createRealtimeClient({ url: 'ws://localhost:3000/realtime', WebSocket, auth: () => 'token' });
```

### 5.3 React

```tsx
import { useState } from 'react';
import { createRealtimeClient } from 'realtime-kit/client';
import { useChannel, useConnectionStatus, useDirect, useRoom } from 'realtime-kit/react';

const rt = createRealtimeClient({ url: 'wss://api.example.com/realtime', auth: () => localStorage.getItem('token') ?? undefined, autoConnect: false });

export function Chat({ roomName }: { roomName: string }) {
  const status = useConnectionStatus(rt);                       // 'open' | 'reconnecting' | ...
  const [msgs, setMsgs] = useState<{ id: string; from: string | null; text: string }[]>([]);

  const room = useRoom<{ text: string }>(rt, roomName, (m) =>   // joins on mount, leaves on unmount, rejoins after reconnects
    setMsgs((x) => [...x, { id: m.id, from: m.from, text: m.data.text }]));
  // room.joined, room.members (live), room.error (e.g. forbidden), room.send(data), room.history({ limit })

  useDirect(rt, (m) => console.log('DM from', m.from, m.data));  // direct messages / notifications
  useChannel(rt, 'orders', (data) => console.log(data));        // plain topic channel

  return (
    <>
      <p>{status} · online: {room.members.join(', ')} {room.error && `(${room.error.message})`}</p>
      <ul>{msgs.map((m) => <li key={m.id}>{m.from}: {m.text}</li>)}</ul>
      <button onClick={() => room.send({ text: 'hi' })}>send</button>
    </>
  );
}
```

Pass `null` as the room/channel to pause. Create the client **once**, outside components (or in a context/provider). Send requests from handlers: `await rt.request('todo.add', { title })`.

### 5.4 Next.js (App Router)

Realtime code lives in a **client component** (`'use client'`). Create the client at module level with `autoConnect: false` and derive the URL from `location` so it works on localhost and production:

```jsx
// app/chat/page.js
'use client';
import { useState } from 'react';
import { createRealtimeClient } from 'realtime-kit/client';
import { useConnectionStatus, useRoom } from 'realtime-kit/react';

const rt = createRealtimeClient({
  url: typeof location !== 'undefined'
    ? `${location.protocol === 'https:' ? 'wss' : 'ws'}://${location.host}/realtime`
    : 'ws://localhost/realtime',                 // placeholder used only while rendering on the server; never connected
  auth: () => localStorage.getItem('token') ?? undefined,
  autoConnect: false,
});

export default function Page() {
  const status = useConnectionStatus(rt);
  const [msgs, setMsgs] = useState([]);
  const room = useRoom(rt, 'public-general', (m) => setMsgs((x) => [...x, `${m.from}: ${m.data.text}`]));
  return <p>status: {status} · members: {room.members.join(',')}</p>;
}
```

The server HTML renders `status: idle`; the browser connects after hydration (no hydration mismatch). This needs the [custom server from 4.4](#44-nextjs-custom-server). If your WebSocket server is a separate service, use `wss://your-realtime-service.com/realtime` and add your Next origin to `allowedOrigins` on that service.

### 5.5 Vue 3 (Vite)

```vue
<script setup lang="ts">
import { ref } from 'vue';
import { createRealtimeClient } from 'realtime-kit/client';
import { useChannel, useConnectionStatus, useDirect, useRoom } from 'realtime-kit/vue';

const rt = createRealtimeClient({ url: 'ws://localhost:3000/realtime', auth: () => 'my-token', autoConnect: false });

const status = useConnectionStatus(rt);                 // Ref<'open' | ...>
const msgs = ref<string[]>([]);
const roomName = ref('public-general');                 // string, ref or getter; changing it switches rooms

const room = useRoom<{ text: string }>(rt, roomName, (m) => msgs.value.push(`${m.from}: ${m.data.text}`));
// room.joined / room.members / room.error are refs; room.send(data), room.history({ limit })

useDirect(rt, (m) => console.log('DM from', m.from, m.data));
useChannel(rt, 'orders', (data) => console.log(data));
</script>

<template>
  <p>{{ status }} · online: {{ room.members.value.join(', ') }}</p>
  <ul><li v-for="(m, i) in msgs" :key="i">{{ m }}</li></ul>
  <button @click="room.send({ text: 'hi' })">send</button>
</template>
```

Composables clean up automatically when the component (or effect scope) is destroyed.

### 5.6 Nuxt 3

**Step 1 – plugin** `plugins/realtime.ts` (a normal universal plugin works):

```ts
import { createRealtimeClient } from 'realtime-kit/client';

export default defineNuxtPlugin(() => {
  const rt = createRealtimeClient({
    url: 'ws://localhost:3000/realtime',          // use runtimeConfig.public for the real URL
    auth: () => useCookie('token').value ?? undefined,
    autoConnect: false,                            // required: the plugin also runs on the server
  });
  return { provide: { rt } };
});
```

**Step 2 – use it** in any page or component (same composables as Vue):

```vue
<script setup lang="ts">
import { useRoom, useDirect } from 'realtime-kit/vue';
const { $rt } = useNuxtApp();
const msgs = ref<string[]>([]);
const room = useRoom<{ text: string }>($rt, 'public-general', (m) => msgs.value.push(`${m.from}: ${m.data.text}`));
useDirect($rt, (m) => console.log(m.data));
</script>
<template><p>{{ room.members.value.join(', ') }}</p></template>
```

SSR output is `idle`; the connection starts on mount in the browser (no hydration mismatch). If the Nuxt dev server and the realtime server are on different origins, add the Nuxt origin (e.g. `http://localhost:3000`) to `allowedOrigins`.

### 5.7 Angular (standalone, v17+)

```ts
import { AsyncPipe } from '@angular/common';
import { Component, signal } from '@angular/core';
import { createRealtimeClient } from 'realtime-kit/client';
import { channel$, direct$, members$, request$, room$, status$ } from 'realtime-kit/angular';

@Component({
  selector: 'app-chat',
  imports: [AsyncPipe],
  template: `
    <p>{{ status$ | async }} · online: {{ (members$ | async)?.join(', ') }}</p>
    <ul>@for (m of msgs(); track m.id) { <li>{{ m.from }}: {{ m.data.text }}</li> }</ul>
    <button (click)="send()">send</button>
  `,
})
export class ChatComponent {
  private rt = createRealtimeClient({ url: 'ws://localhost:3000/realtime', auth: () => 'my-token', autoConnect: false });

  status$ = status$(this.rt);
  members$ = members$(this.rt, 'public-general');          // live member list (joins while subscribed)
  msgs = signal<{ id: string; from: string | null; data: { text: string } }[]>([]);

  constructor() {
    room$<{ text: string }>(this.rt, 'public-general')      // subscribe = join, unsubscribe = leave
      .subscribe((m) => this.msgs.update((x) => [...x, m]));
    direct$(this.rt).subscribe((m) => console.log('DM', m.from, m.data));
  }

  send() { this.rt.room('public-general').send({ text: 'hi' }); }
}
```

Other helpers: `channel$(client, 'orders')`, `presence$(client, room)` (join/leave events), `request$(client, 'method', data)`. For many components, provide the client once with an `InjectionToken` and `inject()` it. End subscriptions with the `async` pipe or `takeUntilDestroyed()`.

---

## 6. Recipes

All recipes use the server options from [4.5](#45-server-options-for-rooms-direct-messages-and-presence) and the client from section 5. The code is shown for React; Vue/Nuxt use the same composables, Angular uses the observables, Nest uses `RealtimeService`, as in the previous section.

### 6.1 Group chat (rooms): join, send, history, who is online

Server: `rooms: { canJoin, onMessage, history }` (section 4.5).

Client:

```tsx
const room = useRoom<{ text: string }>(rt, 'public-general', (m) => addMessage(m));  // live messages
useEffect(() => {
  if (room.joined) room.history({ limit: 50 }).then(setOldMessages);                // older messages from your DB
}, [room.joined]);
room.send({ text: 'hello' });         // everyone in the room gets it, including you and your other tabs
room.members;                         // ['ann', 'bob'] – live, across all server instances
```

Message shape received: `{ id, room, from, data, at }` — `from` is the sender's user id (verified by the server), `id` is unique (use it to de-duplicate history vs live), `at` is a timestamp.

### 6.2 Direct messages (no room)

```tsx
await rt.sendToUser('bob', { text: 'hey bob' });     // -> { id, at, delivered }  (delivered=false if bob is offline)
useDirect(rt, (m) => addDm(m));                       // { id, from, to, data, at }
const past = await rt.directHistory('bob', { limit: 50 });   // from your `direct.history` hook
```

- Bob receives it on **every** tab/device. The sender also gets an echo on all of **their** tabs (including the sending one), so a second tab stays in sync. Use `m.id` to avoid showing it twice.
- Offline users: `delivered` is `false`. Your `direct.onMessage` hook already saved it, so they see it through `directHistory` when they come back.
- Block/allow rules: `direct.canSend`.

### 6.3 Notifications to a user (server → user)

From any server code (a route, a queue worker, a Nest service):

```js
rt.sendToUser(order.userId, { type: 'notification', title: 'Order shipped' });
```

The client receives it through `useDirect` / `rt.onDirect` / `direct$` on all of that user's open tabs and devices. `from` is `null` for messages sent by server code.

### 6.4 "Update in one place, shows up everywhere"

Change data with a request or a normal HTTP route, then push the change to everyone who should see it:

```js
// Server
rt.onRequest('todo.add', async ({ title }, ctx) => {
  const todo = await db.todos.insert({ title, by: ctx.userId });
  rt.publish('todos', { type: 'added', todo });                  // everyone subscribed to the channel
  // or: rt.room(`project-${todo.projectId}`).send({ type: 'todo.added', todo });   // only a project's members
  // or: rt.sendToUser(ownerId, { type: 'todo.added', todo });                      // only one user's devices
  return todo;
});
rt.onRequest('todo.list', () => db.todos.all());
```

```tsx
// Client (any page / any browser / any framework)
useChannel(rt, 'todos', (e) => setTodos((x) => [...x, e.todo]));
useEffect(() => { if (status === 'open') rt.request('todo.list').then(setTodos); }, [status]);   // initial load
await rt.request('todo.add', { title: 'buy milk' });   // appears in every open tab, also on other instances
```

Which one to use:

| Who should see the change | Use |
|---|---|
| Everyone | `rt.publish(channel, data)` + `useChannel` (add `authorize` for who may subscribe) |
| Members of a group | `rt.room(name).send(data)` + `useRoom` |
| One user (all devices) | `rt.sendToUser(userId, data)` + `useDirect` |

Always reload the current state on (re)connect (the `todo.list` call above): messages sent while a client was disconnected are not replayed.

### 6.5 Typing indicators and other temporary events

Send them as ordinary room messages, but don't store them. Mark them with a `type` and skip the database in `onMessage`:

```js
rooms: {
  onMessage: async (ctx, room, data) => {
    if (data?.type === 'typing') return { type: 'typing' };            // broadcast only, nothing saved
    const msg = { type: 'message', text: String(data.text).slice(0, 2000), at: Date.now() };
    await db.messages.insert({ room, from: ctx.userId, ...msg });
    return msg;
  },
}
```

```tsx
room.send({ type: 'typing' });                                           // client: call (throttled) while the user types
useRoom(rt, 'r1', (m) => m.data.type === 'typing' ? showTyping(m.from) : addMessage(m));
```

### 6.6 Read receipts and unread counts

```js
rt.onRequest('chat.markRead', async ({ room, upToId }, ctx) => {
  await db.reads.upsert({ room, user: ctx.userId, upToId });
  rt.room(room).send({ type: 'read', by: ctx.userId, upToId });         // everyone in the room sees "ann read up to X"
  return { ok: true };
});
rt.onRequest('chat.unread', ({ room }, ctx) => db.messages.countUnread(room, ctx.userId));
```

Note: a server-side `rt.room(room).send(...)` does not run your `onMessage` hook and arrives with `from: null`. That is what you want for system events.

### 6.7 Live dashboard / feed

```js
setInterval(async () => rt.publish('stats', await getStats()), 1000);    // server
```

```tsx
useChannel(rt, 'stats', setStats);                                        // client
```

Add `authorize: (ctx, action, ch) => action === 'subscribe' && (ch !== 'stats' || ctx.user.isAdmin)` for private feeds.

---

## 7. Authentication

Authentication happens **during the WebSocket handshake**. If it fails the connection is refused with HTTP 401 and never opens.

**Server:**

```ts
createRealtimeServer({
  server,
  authenticate: async (request, token) => {
    // token = what the client's auth() returned (may be undefined); request = the HTTP upgrade request (cookies, headers)
    return (await verifyJwt(token)) ?? null;       // null (or a thrown error) -> 401
  },
  userId: (user) => user.id,                        // string or number; max 200 chars
});
```

The returned user becomes `ctx.user`; `userId(user)` becomes `ctx.userId`. Without `authenticate`, clients are anonymous (`ctx.user` undefined) and rooms/DMs are unavailable.

**Client — two ways to send credentials:**

| Method | How | When |
|---|---|---|
| **Token** | `createRealtimeClient({ auth: () => token })` | API tokens, JWT, SPA with a separate backend. Sent as a WebSocket subprotocol header, **never in the URL**, so it does not leak into access logs. |
| **Cookie** | nothing extra; browsers send cookies automatically | Same-site web apps. Read `request.headers.cookie` in `authenticate`. |

`auth` is called again on **every reconnect**, so a refreshed token is picked up automatically (it may be `async`).

> Tokens in a subprotocol may contain only URL-safe characters (letters, digits, `. - _`). JWTs are fine. Base64url-encode anything else.

To log a user out everywhere: `rt.disconnectUser(userId)`.

---

## 8. Authorization

There are four separate gates. All are server-side; the client cannot bypass them.

| Gate | Option | Default | Decides |
|---|---|---|---|
| Who may connect | `authenticate` | anyone (anonymous) | 401 on failure |
| Who may join a room | `rooms.canJoin(ctx, room)` | **deny** | `forbidden` on `room.join()` |
| Who may message a user | `direct.canSend(ctx, to)` | allow | `forbidden` on `sendToUser()` |
| Plain channels | `authorize(ctx, 'subscribe' \| 'publish', channel)` | subscribe ✔, client publish ✘ | `forbidden` |

Inside a room, only members can send, read history or list members. Members are checked on every call, not just at join time.

```ts
rooms: {
  canJoin: async (ctx, room) => {
    if (room.startsWith('public-')) return true;
    if (room.startsWith('team-')) return db.teams.isMember(room.slice(5), ctx.userId);
    return false;
  },
}
```

The channel/room name format is `[A-Za-z0-9_.:\-/]{1,128}`.

---

## 9. Scaling with Redis

One server instance keeps its connections in memory. With several instances (load balancer, PM2 cluster, Kubernetes replicas, auto-scaling) a message published on instance A would not reach a user connected to instance B. The Redis adapter fixes that: every instance publishes to Redis Pub/Sub and delivers what it receives to its own clients.

```bash
npm install ioredis
```

```js
import { createRealtimeServer } from 'realtime-kit/server';
import { createRedisAdapter } from 'realtime-kit/redis';

const rt = createRealtimeServer({
  server: httpServer,
  adapter: createRedisAdapter({ url: process.env.REDIS_URL }),   // e.g. redis://user:password@host:6379/0
  // ...the same options as before
});
```

**You provide the Redis credentials**; realtime-kit connects with them.

| Option | Meaning |
|---|---|
| `url` | `redis://user:password@host:6379/0`, or `rediss://...` for TLS (managed Redis: Upstash, Redis Cloud, ElastiCache, Azure...) |
| `options` | Any [ioredis option](https://github.com/redis/ioredis#connect-to-redis): `host`, `port`, `username`, `password`, `db`, `tls`, `sentinels`... |
| `client` | Reuse your own `ioredis` client for publishing (a second connection is created for subscribing; your client is never closed by the adapter) |
| `channel` | Pub/Sub channel name between instances, default `"realtime-kit"`. **Use a different one per app and per environment** (`myapp-prod`, `myapp-staging`) when they share a Redis |
| `onError` | Called on Redis connection errors (ioredis reconnects automatically). Default: `console.error` |

NestJS: `RealtimeModule.forRoot({ ..., adapter: createRedisAdapter({ url: process.env.REDIS_URL }) })`. The adapter is closed on shutdown.

**What works across instances:** channels (`publish`), room messages, direct messages and `sendToUser`, **presence** (`room.members`, join/leave events, `isOnline`, `onlineUsers`). A user with tabs on two instances counts once.

**How presence stays correct:** each instance announces joins/leaves, a new instance asks the others for their state when it starts, and every `clusterSyncMs` (default 10 s) each instance re-broadcasts a snapshot that heals missed events. On graceful shutdown an instance says goodbye; if an instance **crashes**, its users disappear after about `3.5 × clusterSyncMs` (35 s by default).

**Things to know:**
- Redis is used only as a message bus (Pub/Sub). Nothing is stored in it. Local delivery does not depend on Redis; while Redis is unreachable, cross-instance delivery is interrupted (ioredis reconnects and re-subscribes automatically, and the periodic snapshots re-sync presence). This outage path has not been tested here, so verify it for your setup.
- Pub/Sub is at-most-once: a message published while an instance is briefly disconnected from Redis is not replayed. Reload state from your database after reconnects (see 6.4).
- Keep Redis on a private network, require a password/TLS, and treat it as trusted: anyone who can publish to that Redis channel can inject messages to your clients.
- Return values like the recipient count of `publish()` refer to **this instance** only.
- Your `onMessage` / `history` hooks run on the instance that received the request; use a shared database (not process memory) for history.
- Sticky sessions are **not** needed: each WebSocket stays on one instance for its life, and reconnects may land on any instance.
- Not supported yet: Redis Cluster mode (`ioredis.Cluster`). Sentinel should work through ioredis `options` but has not been tested.

To try it locally: `docker run -p 6379:6379 redis`, then start two instances of your server on different ports.

---

## 10. Production deployment

1. **Use `wss://`.** Terminate TLS at your load balancer / reverse proxy (nginx, Caddy, Cloudflare, ALB) or in Node. Browsers block `ws://` from `https://` pages.
2. **Forward the upgrade headers.** nginx example:

   ```nginx
   location /realtime {
     proxy_pass http://127.0.0.1:3000;
     proxy_http_version 1.1;
     proxy_set_header Upgrade $http_upgrade;
     proxy_set_header Connection "upgrade";
     proxy_set_header Host $host;
     proxy_set_header X-Forwarded-For $remote_addr;
     proxy_read_timeout 120s;              # must be larger than heartbeatMs (default 30 s)
   }
   ```

3. **Behind a proxy, set `trustProxy: true`** so per-IP limits use the real client IP from `X-Forwarded-For`. Only when a proxy you control sets that header; otherwise clients can spoof their IP.
4. **Set `allowedOrigins`** to your real front-end origins (full origin, no trailing slash: `https://app.example.com`).
5. **Graceful shutdown:** `await rt.close()` on SIGTERM (Nest: `app.enableShutdownHooks()`).
6. **More than one instance?** Add the [Redis adapter](#9-scaling-with-redis).
7. **Serverless (Vercel, Netlify, Lambda) cannot host the server.** Run it on a long-lived Node process (VPS, Docker, Fly, Railway, Render, ECS...) and connect your front end to it.
8. **Log errors:** pass `onError` to `createRealtimeServer` to send internal errors (hook failures, adapter errors) to your logger. They are printed with `console.error` by default.

---

## 11. API reference

### `createRealtimeServer(options)` — `realtime-kit/server`

| Option | Default | Description |
|---|---|---|
| `server` | – | Existing `http`/`https` server to attach to. |
| `port` | – | If `server` is omitted, listen on this port with a new HTTP server. |
| `path` | `'/realtime'` | URL path that accepts WebSocket upgrades. |
| `allowedOrigins` | `[]` (same host only) | Allowed browser `Origin`s; `'*'` disables the check (not recommended). Requests without an `Origin` (non-browser) are allowed. |
| `authenticate(req, token)` | anonymous | Return user, or `null`/throw → 401. |
| `userId(user)` | – | Stable user id. **Required for rooms, direct messages, presence.** |
| `rooms` | – | `{ canJoin, onMessage, history }` — see 4.5. `canJoin` default **deny**. |
| `direct` | – | `{ canSend, onMessage, history }` — see 4.5. |
| `authorize(ctx, action, channel)` | subscribe ✔, publish ✘ | Permission for plain channels; may be async. |
| `adapter` | – | Cluster adapter, e.g. `createRedisAdapter(...)`. |
| `clusterSyncMs` | `10000` | Presence snapshot interval when `adapter` is set. |
| `onError(err)` | `console.error` | Non-fatal internal errors. |
| `maxPayloadBytes` | `65536` | Max size of one incoming message; larger closes the connection. |
| `rateLimit` | `{ messages: 50, perMs: 1000 }` | Per-connection limit; sustained abuse (2× limit) closes the connection. |
| `maxConnectionsPerIp` | `50` | Extra connections get HTTP 429. |
| `maxSubscriptionsPerClient` | `100` | Channels + rooms per connection. |
| `heartbeatMs` | `30000` | Ping interval; clients that miss a pong are terminated. |
| `maxBufferedBytes` | `1048576` | Slow consumers above this are disconnected. |
| `trustProxy` | `false` | Use `X-Forwarded-For` for the client IP. |

Returned server object:

| Member | Description |
|---|---|
| `publish(channel, data?, { except? })` | Send to subscribers of a plain channel; returns local recipient count. |
| `sendToUser(userId, data?, { from? })` | Direct message/notification to all of a user's connections; `from` is `null` unless given. |
| `room(name)` | `{ send(data), members(), size }` — broadcast into a room from server code (`from: null`, skips your `onMessage` hook). |
| `isOnline(userId)` / `onlineUsers()` | Presence (cluster-wide when an `adapter` is set). |
| `onRequest(method, handler)` | `handler(data, ctx)` → result (or throw `RealtimeError`). Names starting with `rk.` are reserved. |
| `onConnection(cb)` / `onDisconnect(cb)` | `cb(ctx)` where `ctx = { id, user, userId, ip, request }`. |
| `size` | Live connections on this instance. |
| `disconnect(clientId)` / `disconnectUser(userId)` | Close one connection / all of a user's connections. |
| `close()` | Stop, closing all sockets (and the adapter). Returns a promise. |

`RealtimeError(code, message)` — `code` is one of `bad_request`, `forbidden`, `rate_limited`, `not_found`, `internal`. Thrown from request handlers and hooks, it reaches the client as the same code and message; any other error becomes `{ code: 'internal', message: 'internal error' }`.

### `createRedisAdapter(options)` — `realtime-kit/redis`

`{ url?, options?, client?, channel?, onError? }` — see section 9.

### `createRealtimeClient(options)` — `realtime-kit/client`

| Option | Default | Description |
|---|---|---|
| `url` | required | `ws://` / `wss://` URL including the path. |
| `auth` | – | `() => string \| undefined \| Promise<...>`; called on every (re)connect. |
| `reconnect` | `true` | Auto-reconnect with exponential backoff + jitter. |
| `minDelayMs` / `maxDelayMs` | `500` / `15000` | Backoff bounds. |
| `requestTimeoutMs` | `10000` | `request()` rejects with code `timeout` after this. |
| `autoConnect` | `true` | Set `false` for SSR; the framework helpers (or `connect()`) connect later. |
| `WebSocket` | global | Custom constructor (e.g. `ws` on Node < 22). |

| Method | Description |
|---|---|
| `connect()` / `close()` | Start / stop for good (`close` rejects pending requests, disables reconnect). |
| `room(name)` | Room handle, see below. |
| `sendToUser(userId, data)` | → `{ id, at, delivered }`. |
| `onDirect(handler)` | Direct messages and notifications. Returns an unsubscribe function. |
| `directHistory(withUser, { before?, limit? })` | From your `direct.history` hook. |
| `subscribe(channel, handler)` | Plain channel. Returns an unsubscribe function; re-subscribed after reconnect. |
| `publish(channel, data?)` | Plain channel publish; needs server `authorize` permission. |
| `request(method, data?)` | Promise of the server handler's result. |
| `onStatus(cb)` / `onError(cb)` / `status` | Connection state. |

`rt.room(name)` returns:

| Member | Description |
|---|---|
| `join()` | → user ids in the room. Reference-counted; re-joined automatically after reconnects. |
| `leave()` | Leaves when every `join()` has been matched. |
| `send(data)` | → `{ id, at }`. Goes through your `rooms.onMessage` hook. |
| `history({ before?, limit? })` | From your `rooms.history` hook. |
| `members()` | Current user ids. |
| `onMessage(h)` | `{ id, room, from, data, at }` |
| `onPresence(h)` | `{ room, userId, online, at }` — someone came online / went offline. |
| `onMembers(h)` | Full member list whenever it changes. |
| `joined` | Boolean. |

Error codes on the client: the server codes above plus `timeout`, `disconnected`, `closed`.

### Framework helpers

| Package | Exports |
|---|---|
| `realtime-kit/react` | `useRoom(client, room, onMessage?)` → `{ joined, members, error, send, history }` · `useDirect(client, handler)` · `useChannel(client, channel, handler)` · `useConnectionStatus(client)` |
| `realtime-kit/vue` | the same four composables; arguments accept refs/getters, results are refs |
| `realtime-kit/angular` | `room$`, `members$`, `presence$`, `direct$`, `channel$`, `status$`, `request$` |
| `realtime-kit/nest` | `RealtimeModule.forRoot / forRootAsync`, `RealtimeService`, `RealtimeError` |

---

## 12. Security model

| Protection | Default behavior |
|---|---|
| **Cross-site WebSocket hijacking** | `Origin` must match the server host, or be listed in `allowedOrigins`. |
| **Authentication** | Handshake-time via `authenticate`; token travels in a header, not the URL. |
| **Rooms** | Private by default (`canJoin` denies). Only members can send, read history or list members. |
| **Sender identity** | `from` is set by the server from the verified user. Clients cannot impersonate. |
| **Plain channels** | You decide per channel with `authorize`. Client publish is off by default. |
| **Input validation** | Every frame is parsed and validated; invalid frames get `bad_request`. Channel/room names are restricted. |
| **Flooding / abuse** | Payload size limit, per-connection rate limit, per-IP connection cap, subscription cap. |
| **Dead / slow clients** | Heartbeat terminates dead sockets; slow consumers are disconnected (no unbounded memory). |
| **Information leaks** | Unknown errors reach clients as a generic `internal error` (details only in your server log). |

Your responsibility:

- Serve over **`wss://`** in production.
- **Validate and trim `data`** in your `onMessage` / `onRequest` handlers; the library validates the envelope, not your payload shape. Escape/sanitize text when rendering it (frameworks do this by default; avoid `innerHTML` / `v-html` / `dangerouslySetInnerHTML` with message text).
- Write correct `canJoin` / `authorize` rules; never trust ids that come from the client without checking `ctx.userId`.
- Keep Redis private and password-protected.
- Don't put secrets in channel/room names or payloads that other members can see.

---

## 13. Troubleshooting

| Symptom | Cause / fix |
|---|---|
| Status stays `reconnecting`, server returns **401** | `authenticate` rejected the token (or `userId` returned an empty/too-long id). Browsers can't show the HTTP status of a failed WebSocket upgrade, so check the **server log or Network tab**. The client keeps retrying with backoff; call `rt.close()` to stop (e.g. after logout). |
| Server returns **403** | `Origin` not allowed. Add the page's exact origin (`http://localhost:5173`) to `allowedOrigins`. `localhost` and `127.0.0.1` are different origins. |
| Server returns **429** | Too many connections from one IP (`maxConnectionsPerIp`). Behind a proxy set `trustProxy: true`. |
| `room.join()` → `forbidden: cannot join ...` | `rooms.canJoin` missing (default is deny) or returned false. |
| `forbidden: identity required` | Rooms/DMs need `authenticate` **and** `userId`. |
| `forbidden: join <room> first` | You called `send`/`history`/`members` on a room you haven't joined. With the helpers, wait for `room.joined`. |
| `not_found: room history is not configured` | Add `rooms.history` (or `direct.history`). |
| Messages missing after a reconnect | Messages sent while a client was offline are not replayed. Reload history/state when `status` becomes `open` again. |
| Same DM shows twice | The sender gets an echo of their own message. De-duplicate by `m.id`, or render only from `onDirect`. |
| Members list differs between servers | Redis adapter not configured, or different `channel` names per instance. Presence after a crash heals in ~`3.5 × clusterSyncMs`. |
| Connects locally, fails in production | Proxy isn't forwarding `Upgrade`/`Connection` headers, or the page is `https` but the URL is `ws://`. |
| Connection drops after ~60 s idle | Proxy idle timeout is below `heartbeatMs`. Raise the proxy read timeout. |
| `rate_limited` | Client exceeds `rateLimit`. Throttle typing events; batch messages. |
| Connection closes after a large message | Exceeded `maxPayloadBytes` (64 KiB default). |
| Express: nothing connects | You called `app.listen()` instead of `httpServer.listen()`. |
| Next.js: hydration warning or crash on the server | Client created without `autoConnect: false`, or used outside a `'use client'` component. |
| Nuxt / Angular SSR: socket opened on the server | Client created without `autoConnect: false`. |
| TypeScript: `Cannot find module 'realtime-kit/nest'` | Use the latest version; classic `moduleResolution: node` is supported through `typesVersions`. |
| `WebSocket is not defined` | Client code on Node < 22: pass `WebSocket` from `ws`. |
| Nest: `RealtimeService` is `undefined` in a constructor | Add `@Inject(RealtimeService)` if your build tool (esbuild/SWC/vitest) does not emit decorator metadata. |

---

## 14. Tested with & limitations

End-to-end verified with real apps installed from the packed package, in a real browser (Chromium) where applicable:

| Stack | Version tested | What was exercised |
|---|---|---|
| Node (CJS + ESM, global `WebSocket`) | 24 | connect, auth, requests, security checks |
| Express | 4 | + React chat with rooms, history, DMs, live todos, reconnect |
| Redis, 2 server instances | Redis 7.0 (local) | cross-instance channels, rooms, DMs, presence, late-joining instance, graceful stop and crash |
| NestJS (Express + Fastify platforms) | 10 | module, async module, `RealtimeService` (rooms, DM, presence), shutdown |
| React | 18 (Vite) | `useRoom`, `useDirect`, `useChannel`, `useConnectionStatus` |
| Next.js (custom server, dev + production) | 16 | SSR `idle` → connected, rooms, DM |
| Vue (Vite) | 3.5 | composables incl. rooms/DM |
| Nuxt (dev, SSR + hydration) | 3.21 | no hydration mismatch, rooms/DM |
| Angular (strict AOT, dev server) | 22 | observables incl. rooms/DM |

Known limitations:

- **No message storage or replay**: you persist (`onMessage`) and serve history (`history`); messages sent while a client is offline are not re-delivered.
- **At-most-once delivery**: no acknowledgements for pub/sub; use `request()` for things that must be confirmed.
- **Redis**: Pub/Sub only; Redis Cluster mode is not supported; crashed-instance cleanup takes ~`3.5 × clusterSyncMs`. Tested against a local Redis; managed Redis and Sentinel should work through the same `ioredis` options but were not tested here.
- JSON text frames only (no binary).
- Not tested: Nuxt/Angular production builds, Angular SSR, Firefox/Safari, high load, `wss://` terminated inside Node (use a proxy).

## License

MIT © Mohit Singh
