# realtime-kit

Lightweight, secure WebSocket toolkit: **pub/sub + request/response** for Node, Express, NestJS, Next.js, React, Vue, Nuxt, Angular and plain JavaScript.

- **Tiny**: browser client is ~2 kB gzip, zero dependencies. Server uses only [`ws`](https://github.com/websockets/ws).
- **Secure by default**: origin check, handshake auth, channel authorization, rate limit, payload limit, connection caps.
- **Not just chat**: notifications, live dashboards, presence, progress updates, collaborative apps, IoT feeds. Anything that is "server pushes to subscribed clients" or "client asks server".
- **Resilient client**: auto-reconnect with backoff + jitter, automatic re-subscribe, offline queue, request timeouts.

## Contents

1. [Install](#1-install)
2. [Core concepts](#2-core-concepts)
3. [Server setup](#3-server-setup) – Node · Express · NestJS (Express/Fastify) · Next.js custom server
4. [Client setup](#4-client-setup) – Vanilla JS/Node · React · Next.js · Vue · Nuxt · Angular
5. [Authentication](#5-authentication)
6. [Channels, authorization & request handlers](#6-channels-authorization--request-handlers)
7. [Production deployment](#7-production-deployment)
8. [API reference](#8-api-reference)
9. [Security model](#9-security-model)
10. [Troubleshooting](#10-troubleshooting)
11. [Tested with & limitations](#11-tested-with--limitations)

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

Requirements: Node 18+ on the server. In the browser, any modern browser.

> **Which entry do I import?** Backend code imports `realtime-kit/server` (or `/nest`). Frontend code imports `realtime-kit/client` plus your framework helper. **Never import `/server` in frontend code.**

---

## 2. Core concepts

```
 Browser / Node client  ──ws://──▶  Your HTTP server  (Express / Nest / Next / plain Node)
   rt.subscribe('news')                createRealtimeServer({ server })
   rt.request('getUser')               rt.publish('news', data)
                                       rt.onRequest('getUser', handler)
```

| Concept | Meaning |
|---|---|
| **Channel** | A named topic, e.g. `public:news`, `user:42`, `room:7`. Allowed characters: letters, digits, `_ . : - /`, max 128 chars. |
| **Subscribe / publish** | Clients subscribe to channels. The server publishes to them. Client-side publish is **denied unless you allow it** (see [authorization](#6-channels-authorization--request-handlers)). |
| **Request / response** | Client calls `rt.request('method', data)`, server handler registered with `onRequest('method', fn)` returns the result. Like a tiny RPC. |
| **Endpoint** | The server accepts WebSocket upgrades on one path, default `/realtime`. |

The protocol is JSON text frames. Binary frames are rejected.

---

## 3. Server setup

The server **attaches to an existing HTTP server** (so it shares your port, TLS and process) or creates its own.

### 3.1 Plain Node

```js
import http from 'node:http';
import { createRealtimeServer } from 'realtime-kit/server';

const httpServer = http.createServer((req, res) => res.end('ok'));

const rt = createRealtimeServer({
  server: httpServer,
  allowedOrigins: ['https://app.example.com'],
});

rt.publish('public:news', { title: 'Hello' });

httpServer.listen(3000);
```

Without `server`, pass `port` and realtime-kit creates its own HTTP server: `createRealtimeServer({ port: 3000 })`.

CommonJS works too: `const { createRealtimeServer } = require('realtime-kit/server');`

### 3.2 Express

```js
import http from 'node:http';
import express from 'express';
import { createRealtimeServer } from 'realtime-kit/server';

const app = express();
const httpServer = http.createServer(app);          // 1. wrap Express in an http server

const rt = createRealtimeServer({                    // 2. attach realtime-kit to that server
  server: httpServer,
  allowedOrigins: ['http://localhost:5173', 'https://app.example.com'],
  authenticate: (req, token) => verifyToken(token),  // return a user object, or null to reject (401)
  authorize: (ctx, action, channel) =>
    action === 'subscribe' && (channel.startsWith('public:') || channel === `user:${ctx.user.id}`),
});

rt.onRequest('whoami', (_data, ctx) => ctx.user);

app.get('/api/notify', (_req, res) => {
  rt.publish('public:news', { at: Date.now() });
  res.json({ ok: true });
});

httpServer.listen(3000);                             // 3. listen on httpServer, NOT app.listen()
```

> Use `httpServer.listen()`. `app.listen()` creates a different server that realtime-kit is not attached to.

### 3.3 NestJS (Express or Fastify adapter)

Install: `npm i realtime-kit @nestjs/common @nestjs/core reflect-metadata`

**Step 1 – register the module** (`app.module.ts`):

```ts
import { Module } from '@nestjs/common';
import { RealtimeModule } from 'realtime-kit/nest';

@Module({
  imports: [
    RealtimeModule.forRoot({
      allowedOrigins: ['https://app.example.com'],
      authenticate: (req, token) => (token === 'secret' ? { id: 1 } : null),
    }),
  ],
})
export class AppModule {}
```

Options that need `ConfigService` or other async values:

```ts
RealtimeModule.forRootAsync({
  imports: [ConfigModule],
  inject: [ConfigService],
  useFactory: (config: ConfigService) => ({
    allowedOrigins: config.get<string>('WS_ORIGINS')!.split(','),
    authenticate: (req, token) => jwtService.verifyOrNull(token),
  }),
});
```

**Step 2 – use the service** (`chat.service.ts`):

```ts
import { Injectable, OnModuleInit } from '@nestjs/common';
import { RealtimeService, RealtimeError } from 'realtime-kit/nest';

@Injectable()
export class ChatService implements OnModuleInit {
  constructor(private readonly rt: RealtimeService) {}

  onModuleInit() {
    // Safe to register here: calls are queued until the HTTP server is ready.
    this.rt.onRequest('ping', () => 'pong');
    this.rt.onRequest('admin.kick', (_d, ctx) => {
      throw new RealtimeError('forbidden', 'admins only'); // code + message reach the client
    });
  }

  notify(userId: string) {
    return this.rt.publish(`user:${userId}`, { hello: true }); // returns recipient count
  }
}
```

**Step 3 – bootstrap** (`main.ts`). Works unchanged for both platforms:

```ts
const app = await NestFactory.create(AppModule);                       // Express (default)
// or: NestFactory.create<NestFastifyApplication>(AppModule, new FastifyAdapter());
app.enableShutdownHooks();                                             // closes sockets on SIGTERM
await app.listen(3000);
```

The WebSocket endpoint is then `ws://host:3000/realtime`.

### 3.4 Next.js (custom server)

Next.js route handlers and Vercel/serverless functions **cannot hold WebSocket connections**. To run WebSockets next to Next.js, use a **custom server** (self-hosted Node, Docker, VPS), or host realtime-kit as a separate service and connect to it from Next.

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
    server,                                   // same server and port as Next.js
    authenticate: (req, token) => verifyToken(token),
  });
  rt.onRequest('whoami', (_d, ctx) => ctx.user);

  server.listen(3000);
});
```

Change `package.json` scripts: `"dev": "node server.js"`, `"start": "NODE_ENV=production node server.js"`. Next's own HMR socket keeps working in dev; realtime-kit only handles upgrades on its own `path`.

Because the page and the socket share one origin, you don't need `allowedOrigins`.

---

## 4. Client setup

All clients use the same core:

```ts
import { createRealtimeClient } from 'realtime-kit/client';

const rt = createRealtimeClient({
  url: 'wss://api.example.com/realtime',   // ws:// for local dev, wss:// in production
  auth: () => getAccessToken(),            // optional, see Authentication
});

const unsubscribe = rt.subscribe('public:news', (data) => console.log(data));
const user = await rt.request('whoami');
rt.onStatus((s) => console.log(s));        // 'idle' | 'connecting' | 'open' | 'reconnecting' | 'closed'
rt.onError((e) => console.warn(e.code, e.message));
// later: unsubscribe(); rt.close();
```

> **SSR rule (Next, Nuxt, Angular SSR):** frameworks that render on the server run your module/component code there too. Create the client with `autoConnect: false`. The framework helpers (`useChannel`, `channel$`) connect for you in the browser.

### 4.1 Vanilla JS / any bundler

Use the snippet above. In a `<script type="module">` page served by a bundler (Vite, webpack, esbuild) it works as is.

### 4.2 Node scripts (client in Node)

Node 22+ has a global `WebSocket`. On older Node, pass the `ws` package:

```js
import WebSocket from 'ws';
const rt = createRealtimeClient({ url: 'ws://localhost:3000/realtime', WebSocket });
```

### 4.3 React

```tsx
import { useState } from 'react';
import { createRealtimeClient } from 'realtime-kit/client';
import { useChannel, useConnectionStatus } from 'realtime-kit/react';

// Create ONCE, outside components (or in a context/provider)
const rt = createRealtimeClient({
  url: 'wss://api.example.com/realtime',
  auth: () => localStorage.getItem('token') ?? undefined,
  autoConnect: false,
});

export function News() {
  const status = useConnectionStatus(rt);        // re-renders on change
  const [items, setItems] = useState<string[]>([]);

  useChannel<{ title: string }>(rt, 'public:news', (d) => setItems((x) => [...x, d.title]));
  // pass null/undefined as channel to pause the subscription

  return <p>{status}: {items.join(', ')}</p>;
}
```

`useChannel` subscribes on mount, unsubscribes on unmount, and always calls your latest handler. It also calls `rt.connect()`, so `autoConnect: false` is fine and is the safe choice.

Send requests from event handlers: `await rt.request('chat.send', { text })`.

### 4.4 Next.js (App Router)

The realtime code must live in a **client component** (`'use client'`). Create the client at module level with `autoConnect: false` and derive the URL from `location`, so the same code works on localhost and production:

```jsx
// app/page.js
'use client';
import { useState } from 'react';
import { createRealtimeClient } from 'realtime-kit/client';
import { useChannel, useConnectionStatus } from 'realtime-kit/react';

const rt = createRealtimeClient({
  url: typeof location !== 'undefined'
    ? `${location.protocol === 'https:' ? 'wss' : 'ws'}://${location.host}/realtime`
    : 'ws://localhost/realtime',                 // placeholder used only during SSR, never connected
  auth: () => localStorage.getItem('token') ?? undefined,
  autoConnect: false,
});

export default function Page() {
  const status = useConnectionStatus(rt);
  const [tick, setTick] = useState(0);
  useChannel(rt, 'public:ticks', (d) => setTick(d.n));
  return <p>status: {status} · tick: {tick}</p>;
}
```

The server HTML renders `status: idle`; the browser connects after hydration. This requires the [custom server from 3.4](#34-nextjs-custom-server). If your WebSocket server is a separate service, replace the URL with `wss://your-realtime-service.com/realtime` and add your Next origin to `allowedOrigins` on that service.

### 4.5 Vue 3 (Vite)

```vue
<script setup lang="ts">
import { ref } from 'vue';
import { createRealtimeClient } from 'realtime-kit/client';
import { useChannel, useConnectionStatus } from 'realtime-kit/vue';

const rt = createRealtimeClient({ url: 'ws://localhost:3000/realtime', auth: () => 'my-token' });

const status = useConnectionStatus(rt);               // Ref<'open' | ...>
const messages = ref<string[]>([]);
const room = ref('room:1');

// channel can be a string, a ref, or a getter; changing it re-subscribes
useChannel<{ text: string }>(rt, room, (m) => messages.value.push(m.text));

const send = (text: string) => rt.request('chat.send', { text });
</script>

<template>
  <p>{{ status }}</p>
  <ul><li v-for="(m, i) in messages" :key="i">{{ m }}</li></ul>
</template>
```

Composables clean up automatically when the component (or effect scope) is destroyed.

### 4.6 Nuxt 3

**Step 1 – plugin** `plugins/realtime.ts` (a normal universal plugin works):

```ts
import { createRealtimeClient } from 'realtime-kit/client';

export default defineNuxtPlugin(() => {
  const rt = createRealtimeClient({
    url: 'ws://localhost:3000/realtime',        // use runtimeConfig.public for the real URL
    auth: () => useCookie('token').value ?? undefined,
    autoConnect: false,                          // required: the plugin also runs on the server
  });
  return { provide: { rt } };
});
```

**Step 2 – use it in a page or component:**

```vue
<script setup lang="ts">
import { useChannel, useConnectionStatus } from 'realtime-kit/vue';

const { $rt } = useNuxtApp();
const status = useConnectionStatus($rt);
const tick = ref(0);
useChannel<{ n: number }>($rt, 'public:ticks', (d) => (tick.value = d.n));
</script>

<template><p>{{ status }} · {{ tick }}</p></template>
```

SSR output is `idle`; the connection starts on mount in the browser, so there is no hydration mismatch. Nuxt dev server and the realtime server are separate origins unless you proxy: add the Nuxt origin (e.g. `http://localhost:3000`) to `allowedOrigins` on the realtime server.

### 4.7 Angular (standalone, v17+)

```ts
import { AsyncPipe } from '@angular/common';
import { Component, signal } from '@angular/core';
import { createRealtimeClient } from 'realtime-kit/client';
import { channel$, request$, status$ } from 'realtime-kit/angular';

@Component({
  selector: 'app-root',
  imports: [AsyncPipe],
  template: `
    <p>status: {{ status$ | async }}</p>
    <p>tick: {{ (ticks$ | async)?.n }}</p>
    <button (click)="whoami()">whoami</button> {{ me() }}
  `,
})
export class App {
  private rt = createRealtimeClient({
    url: 'ws://localhost:3000/realtime',
    auth: () => 'my-token',
    autoConnect: false,                    // channel$ connects on subscribe
  });

  status$ = status$(this.rt);              // emits current status immediately
  ticks$ = channel$<{ n: number }>(this.rt, 'public:ticks');   // subscribe = join, unsubscribe = leave
  me = signal('');

  whoami() { request$<{ name: string }>(this.rt, 'whoami').subscribe((u) => this.me.set(u.name)); }
}
```

For apps with many components, provide the client once as a service:

```ts
export const RT = new InjectionToken<RealtimeClient>('RT');
// app.config.ts → providers: [{ provide: RT, useFactory: () => createRealtimeClient({ url, autoConnect: false }) }]
// component → private rt = inject(RT);
```

Use `takeUntilDestroyed()` (or the `async` pipe) so subscriptions end with the component.

---

## 5. Authentication

Authentication happens **during the WebSocket handshake**. If it fails, the connection is refused with HTTP 401 and never opens.

**Server:**

```ts
createRealtimeServer({
  server,
  authenticate: async (request, token) => {
    // `token` = what the client's `auth()` returned (may be undefined)
    // `request` = the HTTP upgrade request: read cookies/headers from it
    const user = await verifyJwt(token);
    return user ?? null;           // null (or a thrown error) → 401
  },
});
```

The returned object becomes `ctx.user` in `authorize`, `onRequest`, `onConnection`.

**Client — two ways to send credentials:**

| Method | How | When |
|---|---|---|
| **Token** | `createRealtimeClient({ auth: () => token })` | API tokens, JWT, SPA with separate backend. Sent as a WebSocket subprotocol header, **never in the URL**, so it does not leak into access logs. |
| **Cookie** | nothing extra; browser sends cookies automatically | Same-site web apps. Read `request.headers.cookie` in `authenticate`. |

`auth` is called again on **every reconnect**, so a refreshed token is picked up automatically. It can be `async`.

Without `authenticate`, every client is accepted as anonymous (`ctx.user` is `undefined`). Fine for public feeds; not for anything private.

> Tokens in a subprotocol may contain only URL-safe characters (letters, digits, `. - _`). JWTs are fine. If your token has other characters, base64url-encode it first.

---

## 6. Channels, authorization & request handlers

### Authorization

```ts
createRealtimeServer({
  server,
  authorize: (ctx, action, channel) => {
    // action is 'subscribe' | 'publish'
    if (channel.startsWith('public:')) return action === 'subscribe';
    if (channel === `user:${ctx.user.id}`) return action === 'subscribe';
    if (channel.startsWith('room:')) return isMember(ctx.user.id, channel);   // may be async
    return false;
  },
});
```

**Default policy:** subscribe is allowed, client publish is denied. Always set `authorize` for private data, otherwise any authenticated client can subscribe to any channel name it can guess.

To let clients publish (e.g. typing indicators), return `true` for `action === 'publish'` on specific channels. Client-published messages go to other subscribers, not back to the sender.

### Publishing from the server

```ts
const delivered = rt.publish('public:news', { title: 'Hi' });   // number of recipients
rt.publish('room:7', { text: 'hi' }, { except: ctx.id });       // skip one client
```

### Request handlers

```ts
rt.onRequest('getOrder', async (data, ctx) => {
  const order = await db.order.find(data.id);
  if (order.userId !== ctx.user.id) throw new RealtimeError('forbidden', 'not your order');
  return order;                                                   // sent back to the caller
});
```

```ts
const order = await rt.request('getOrder', { id: 5 });           // client side (10 s timeout by default)
```

Errors: throw `RealtimeError(code, message)` (imported from `realtime-kit/server` or `/nest`) and the client's promise rejects with the same `code` and `message`. Any other error becomes `{ code: 'internal', message: 'internal error' }`: internal details are never leaked.

Codes: `bad_request`, `forbidden`, `rate_limited`, `not_found`, `internal` (client also produces `timeout`, `disconnected`, `closed`).

### Presence / connection hooks

```ts
rt.onConnection((ctx) => rt.publish('public:presence', { online: rt.size }));
rt.onDisconnect((ctx) => rt.publish('public:presence', { online: rt.size }));
rt.disconnect(clientId);          // kick a client
```

---

## 7. Production deployment

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

3. **Behind a proxy, set `trustProxy: true`** so per-IP limits use the real client IP from `X-Forwarded-For`. Only enable it when a proxy you control sets that header; otherwise clients can spoof their IP.
4. **Set `allowedOrigins`** to your real front-end origins (full origin, no trailing slash: `https://app.example.com`).
5. **Graceful shutdown:** `await rt.close()` on SIGTERM (Nest: `app.enableShutdownHooks()`).
6. **Scaling:** state (channels, connections) is in memory per process. With multiple instances, a `publish` reaches only clients connected to that instance. Use sticky sessions and one instance, or fan out across instances yourself (e.g. Redis pub/sub calling `rt.publish`). A built-in Redis adapter is not included yet.
7. **Serverless (Vercel, Netlify, Lambda) cannot host the server.** Run it on a long-lived Node process (VPS, Docker, Fly, Railway, Render, ECS...) and connect your front end to it.

---

## 8. API reference

### `createRealtimeServer(options)` — `realtime-kit/server`

| Option | Default | Description |
|---|---|---|
| `server` | – | Existing `http`/`https` server to attach to. |
| `port` | – | If `server` is omitted, listen on this port with a new HTTP server. |
| `path` | `'/realtime'` | URL path that accepts WebSocket upgrades. |
| `allowedOrigins` | `[]` (same host only) | Allowed browser `Origin`s; `'*'` disables the check (not recommended). Requests without an `Origin` (non-browser) are allowed. |
| `authenticate(req, token)` | anonymous | Return user, or `null`/throw → 401. |
| `authorize(ctx, action, channel)` | subscribe ✔, publish ✘ | Per-channel permission; may be async. |
| `maxPayloadBytes` | `65536` | Max size of one incoming message; larger closes the connection. |
| `rateLimit` | `{ messages: 50, perMs: 1000 }` | Per-connection limit; sustained abuse (2× limit) closes the connection. |
| `maxConnectionsPerIp` | `50` | Extra connections get HTTP 429. |
| `maxSubscriptionsPerClient` | `100` | |
| `heartbeatMs` | `30000` | Ping interval; clients that miss a pong are terminated. |
| `maxBufferedBytes` | `1048576` | Slow consumers above this are disconnected. |
| `trustProxy` | `false` | Use `X-Forwarded-For` for client IP. |

Returns:

| Member | Description |
|---|---|
| `publish(channel, data?, { except? })` | Send to subscribers; returns recipient count. |
| `onRequest(method, handler)` | `handler(data, ctx)` → result (or throw `RealtimeError`). |
| `onConnection(cb)` / `onDisconnect(cb)` | `cb(ctx)` where `ctx = { id, user, ip, request }`. |
| `size` | Number of live connections. |
| `disconnect(clientId, code?, reason?)` | Close one client. |
| `close()` | Stop, closing all sockets. Returns a promise. |

NestJS `RealtimeService` exposes the same methods (`publish`, `onRequest`, `onConnection`, `onDisconnect`, `disconnect`, `size`). `RealtimeModule.forRoot(options)` / `forRootAsync({ imports, inject, useFactory })` take the same options except `server`/`port`.

### `createRealtimeClient(options)` — `realtime-kit/client`

| Option | Default | Description |
|---|---|---|
| `url` | required | `ws://` / `wss://` URL including the path. |
| `auth` | – | `() => string \| undefined \| Promise<...>`; called on every (re)connect. |
| `reconnect` | `true` | Auto-reconnect with exponential backoff + jitter. |
| `minDelayMs` / `maxDelayMs` | `500` / `15000` | Backoff bounds. |
| `requestTimeoutMs` | `10000` | `request()` rejects with code `timeout` after this. |
| `autoConnect` | `true` | Set `false` for SSR; call `connect()` or use the framework helpers. |
| `WebSocket` | global | Custom constructor (e.g. `ws` on Node < 22). |

| Method | Description |
|---|---|
| `connect()` / `close()` | Start / stop for good (`close` rejects pending requests, disables reconnect). |
| `subscribe(channel, handler)` | Returns an unsubscribe function. Re-subscribed automatically after reconnect. |
| `publish(channel, data?)` | Needs server `authorize` permission. Queued while offline. |
| `request(method, data?)` | Promise of the server handler's result. |
| `onStatus(cb)` / `onError(cb)` | Return an unsubscribe function. |
| `status` | Current status. |

### Framework helpers

| Package | Exports |
|---|---|
| `realtime-kit/react` | `useChannel(client, channel, handler)`, `useConnectionStatus(client)` |
| `realtime-kit/vue` | `useChannel(client, channel \| ref \| getter, handler)`, `useConnectionStatus(client)` |
| `realtime-kit/angular` | `channel$(client, channel)`, `status$(client)`, `request$(client, method, data?)` |
| `realtime-kit/nest` | `RealtimeModule`, `RealtimeService`, `RealtimeError` |

---

## 9. Security model

What you get by default and what stays your responsibility:

| Protection | Default behavior |
|---|---|
| **Cross-site WebSocket hijacking** | `Origin` must match the server host, or be listed in `allowedOrigins`. |
| **Authentication** | Handshake-time via `authenticate`; token travels in a header, not the URL. |
| **Authorization** | You decide per channel with `authorize`. Client publish is off by default. |
| **Input validation** | Every frame is parsed and validated; invalid frames get `bad_request`. Channel names are restricted. |
| **Flooding / abuse** | Payload size limit, per-connection rate limit, per-IP connection cap, subscription cap. |
| **Dead / slow clients** | Heartbeat terminates dead sockets; slow consumers are disconnected (no unbounded memory). |
| **Information leaks** | Unknown errors reach clients as a generic `internal error`. |

Your responsibility:

- Serve over **`wss://`** in production.
- **Validate the `data`** inside your `onRequest` handlers and in anything you publish (the library only checks the envelope, not your payload shape).
- Write correct `authorize` rules for private channels; never trust a channel name or user id coming from the client without checking `ctx.user`.
- Don't put secrets in channel names or published payloads that other subscribers can see.

---

## 10. Troubleshooting

| Symptom | Cause / fix |
|---|---|
| Status stays `reconnecting`, server returns **401** | `authenticate` rejected the token. Check what `auth()` returns. Browsers can't show the HTTP status for failed WebSocket upgrades, so check the **server log or Network tab**. The client keeps retrying with backoff; call `rt.close()` if you want it to stop (e.g. user logged out). |
| Server returns **403** | `Origin` not allowed. Add the page's exact origin (`http://localhost:5173`) to `allowedOrigins`. |
| Server returns **429** | Too many connections from the same IP (`maxConnectionsPerIp`). Behind a proxy, set `trustProxy: true` so IPs are not all the proxy's. |
| Connects locally, fails in production | Proxy isn't forwarding `Upgrade`/`Connection` headers (see nginx example), or page is `https` but URL is `ws://` (use `wss://`). |
| Connection drops after ~60 s idle | Proxy idle timeout is below `heartbeatMs`. Raise the proxy's read timeout or lower `heartbeatMs`. |
| `error: forbidden` on subscribe/publish | Your `authorize` returned false, or you tried to publish without allowing it. |
| `error: rate_limited` | Client exceeds `rateLimit`. Batch messages or raise the limit. |
| Connection closes right after sending a large message | Exceeded `maxPayloadBytes` (64 KiB default). |
| Express: nothing connects | You called `app.listen()` instead of `httpServer.listen()`. |
| Next.js: hydration warning or crash on the server | The client was created without `autoConnect: false`, or used outside a `'use client'` component. |
| Nuxt/Angular SSR: socket opened on the server | Client created without `autoConnect: false`. |
| `Cannot find module 'realtime-kit/nest'` (TypeScript) | Update to the latest `realtime-kit`; older tsconfig `moduleResolution: node` is supported through `typesVersions`. |
| `window is not defined` / `WebSocket is not defined` | Running client code on a runtime without a global `WebSocket` (Node < 22): pass `WebSocket` from `ws`. |
| Nest: `RealtimeService` is `undefined` in a constructor | Add `@Inject(RealtimeService)` if your build tool (esbuild/SWC/vitest) does not emit decorator metadata. |

---

## 11. Tested with & limitations

End-to-end verified (real apps installed from the packed tarball, in a real browser where applicable):

| Stack | Version tested |
|---|---|
| Node (CJS + ESM, global `WebSocket`) | 24 |
| Express | 4 |
| NestJS (Express + Fastify platforms) | 10 |
| React | 18 (Vite) |
| Next.js (custom server, dev + production) | 16 |
| Vue (Vite) | 3.5 |
| Nuxt (dev, SSR + hydration) | 3.21 |
| Angular (strict AOT, dev server) | 22 |

Known limitations:

- **No built-in multi-instance fan-out** (no Redis adapter yet): see [Scaling](#7-production-deployment).
- **JSON text only**: no binary frames, no message history/replay, no delivery acknowledgements for pub/sub.
- Not tested yet: Nuxt/Angular production builds and SSR for Angular, Safari/Firefox, load at scale, and `wss://` termination inside Node (use a proxy).

## License

MIT © Mohit Singh
