# realtime-kit

Lightweight, secure WebSocket toolkit: **pub/sub + request/response**. One dependency (`ws`, server only). Client has zero dependencies.

```bash
npm i realtime-kit
```

| Import | Runs on | Use for |
|---|---|---|
| `realtime-kit/server` | Node 18+ | Node, Express, NestJS, Next custom server |
| `realtime-kit/client` | Browser / Node | React, Next, Vue, Nuxt, Angular, vanilla |
| `realtime-kit/react` | React 17+ | `useChannel`, `useConnectionStatus` |
| `realtime-kit/vue` | Vue 3.3+, Nuxt 3 | `useChannel`, `useConnectionStatus` composables |
| `realtime-kit/angular` | Angular (needs `rxjs`) | `channel$`, `status$`, `request$` observables |
| `realtime-kit/nest` | NestJS 9+ | `RealtimeModule`, `RealtimeService` |

## Server

```ts
import http from 'node:http';
import express from 'express';
import { createRealtimeServer } from 'realtime-kit/server';

const app = express();
const httpServer = http.createServer(app);

const rt = createRealtimeServer({
  server: httpServer,                       // works with Express/Nest: pass their http server
  path: '/realtime',
  allowedOrigins: ['https://app.example.com'],
  authenticate: (req, token) => verifyJwt(token),   // return user or null (-> 401)
  authorize: (ctx, action, channel) => channel.startsWith(`user:${ctx.user.id}`) || channel.startsWith('public:'),
});

rt.onRequest('getTime', () => Date.now());
rt.publish('public:news', { title: 'Hello' });

httpServer.listen(3000);
```

NestJS: call `createRealtimeServer({ server: app.getHttpServer() })` after `app.init()`.

## Client

```ts
import { createRealtimeClient } from 'realtime-kit/client';

const rt = createRealtimeClient({
  url: 'wss://api.example.com/realtime',
  auth: () => getAccessToken(),   // sent as a subprotocol, never in the URL
});

const off = rt.subscribe('public:news', (data) => console.log(data));
const time = await rt.request<number>('getTime');
```

SSR frameworks (Next, Nuxt, Angular Universal): create with `autoConnect: false` and call `connect()` in the browser (`useEffect`, `onMounted`, `ngOnInit`).

### React

```tsx
import { useChannel, useConnectionStatus } from 'realtime-kit/react';

useChannel(rt, 'public:news', (data) => setNews(data));
const status = useConnectionStatus(rt);
```

### Vue / Nuxt

```ts
import { useChannel, useConnectionStatus } from 'realtime-kit/vue';

const status = useConnectionStatus(rt);               // Ref<'open' | ...>
useChannel(rt, () => `room:${roomId.value}`, (msg) => messages.value.push(msg));
```

Nuxt / SSR: create the client with `autoConnect: false` (works in a normal universal plugin). `useChannel` connects on mount, so the server never opens a socket and the first client render matches the SSR HTML (no hydration mismatch).

### Angular

```ts
import { channel$, status$, request$ } from 'realtime-kit/angular';

news$ = channel$<News>(this.rt, 'public:news');       // template: news$ | async
status$ = status$(this.rt);
time$ = request$<number>(this.rt, 'getTime');
```

Provide the client once (e.g. `{ provide: RT, useFactory: () => createRealtimeClient({ url, autoConnect: false }) }`); `channel$` connects on subscribe, so it is SSR-safe.

### NestJS

```ts
import { RealtimeModule, RealtimeService } from 'realtime-kit/nest';

@Module({
  imports: [RealtimeModule.forRoot({ allowedOrigins: ['https://app.example.com'], authenticate: (req, token) => jwt.verify(token) })],
})
export class AppModule {}

@Injectable()
export class ChatService implements OnModuleInit {
  constructor(private readonly rt: RealtimeService) {}
  onModuleInit() { this.rt.onRequest('ping', () => 'pong'); }   // safe before the HTTP server starts
  notify(userId: string) { this.rt.publish(`user:${userId}`, { hello: true }); }
}
```

Use `forRootAsync({ imports: [ConfigModule], inject: [ConfigService], useFactory })` for env-based options. Add `app.enableShutdownHooks()` for graceful close.

## Security defaults

- Origin check (same-host only unless `allowedOrigins` is set)
- Auth at handshake; token via subprotocol, not URL
- Clients **cannot publish** unless `authorize` allows it; subscribe is allowed unless denied
- Channel name validation, max payload (64 KiB), per-connection rate limit, per-IP connection cap, subscription cap
- Heartbeat + slow-consumer disconnect; internal errors are never sent to clients
- Use `wss://` in production (terminate TLS at your proxy or server). Set `trustProxy: true` only behind a trusted proxy.

Next.js on Vercel (serverless) cannot host WebSocket servers; run the server as a separate service or a custom Node server.

## License

MIT
