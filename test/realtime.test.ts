import { createServer, type Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import WebSocket from 'ws';
import { createRealtimeServer, RealtimeError, type RealtimeServer } from '../src/server';
import { RealtimeClient } from '../src/client';

let http: Server;
let rt: RealtimeServer<{ name: string }>;
let url: string;
const clients: RealtimeClient[] = [];

const mk = (opts: Partial<ConstructorParameters<typeof RealtimeClient>[0]> = {}) => {
  const c = new RealtimeClient({ url, WebSocket: WebSocket as any, ...opts });
  clients.push(c);
  return c;
};
const open = (c: RealtimeClient) =>
  new Promise<void>((res) => (c.status === 'open' ? res() : void c.onStatus((s) => s === 'open' && res())));
const wait = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function start(opts: Parameters<typeof createRealtimeServer<{ name: string }>>[0] = {}) {
  http = createServer();
  rt = createRealtimeServer({ server: http, ...opts });
  await new Promise<void>((r) => http.listen(0, r));
  url = `ws://127.0.0.1:${(http.address() as AddressInfo).port}/realtime`;
}

afterEach(async () => {
  clients.splice(0).forEach((c) => c.close());
  await rt.close();
  await new Promise<void>((r) => http.close(() => r()));
});

describe('pub/sub', () => {
  beforeEach(() => start());

  it('delivers server publishes to subscribers', async () => {
    const c = mk();
    const got: unknown[] = [];
    c.subscribe('news', (d) => got.push(d));
    await open(c);
    await wait(30);
    expect(rt.publish('news', { hi: 1 })).toBe(1);
    await wait(30);
    expect(got).toEqual([{ hi: 1 }]);
  });

  it('denies client publish by default', async () => {
    const a = mk();
    const errors: string[] = [];
    a.onError((e) => errors.push(e.code));
    await open(a);
    a.publish('news', 1);
    await wait(50);
    expect(errors).toEqual(['forbidden']);
  });

  it('handles request/response and hides internal errors', async () => {
    rt.onRequest('add', (d: any) => d.a + d.b);
    rt.onRequest('boom', () => { throw new Error('secret db password'); });
    rt.onRequest('nope', () => { throw new RealtimeError('forbidden', 'not yours'); });
    const c = mk();
    await open(c);
    expect(await c.request('add', { a: 1, b: 2 })).toBe(3);
    await expect(c.request('boom')).rejects.toMatchObject({ code: 'internal', message: 'internal error' });
    await expect(c.request('nope')).rejects.toMatchObject({ code: 'forbidden' });
    await expect(c.request('missing')).rejects.toMatchObject({ code: 'not_found' });
  });

  it('resubscribes after reconnect', async () => {
    let firstId = '';
    rt.onConnection((ctx) => { firstId ||= ctx.id; });
    const c = mk({ minDelayMs: 20, maxDelayMs: 40 });
    const got: unknown[] = [];
    c.subscribe('ch', (d) => got.push(d));
    await open(c);
    await wait(30);
    // kick the client from the server; it should reconnect and resubscribe
    const reopened = new Promise<void>((r) => {
      const off = c.onStatus((s) => { if (s === 'open') { off(); r(); } });
    });
    rt.disconnect(firstId, 1001);
    await reopened;
    await wait(30);
    rt.publish('ch', 'again');
    await wait(30);
    expect(got).toEqual(['again']);
  });
});

describe('security', () => {
  it('rejects unauthenticated clients and accepts valid tokens', async () => {
    await start({ authenticate: (_req, token) => (token === 'good' ? { name: 'ann' } : null) });
    const bad = mk({ reconnect: false });
    const status = new Promise((r) => bad.onStatus((s) => s === 'closed' && r(s)));
    await status;
    expect(rt.size).toBe(0);

    const ok = mk({ auth: () => 'good' });
    await open(ok);
    expect(rt.size).toBe(1);
  });

  it('rejects disallowed Origin', async () => {
    await start({ allowedOrigins: ['https://app.example.com'] });
    const ws = new WebSocket(url, 'realtime-kit', { headers: { origin: 'https://evil.com' } });
    const code = await new Promise((r) => ws.on('unexpected-response', (_q, res) => r(res.statusCode)));
    expect(code).toBe(403);
  });

  it('enforces channel authorization', async () => {
    await start({ authorize: (_c, action, ch) => action === 'subscribe' && ch.startsWith('public:') });
    const c = mk();
    const errors: string[] = [];
    c.onError((e) => errors.push(e.code));
    await open(c);
    c.subscribe('private:x', () => {});
    await wait(50);
    expect(errors).toEqual(['forbidden']);
  });

  it('rate limits floods', async () => {
    await start({ rateLimit: { messages: 5, perMs: 1000 } });
    const c = mk();
    const errors: string[] = [];
    c.onError((e) => errors.push(e.code));
    await open(c);
    for (let i = 0; i < 8; i++) c.publish('x', i);
    await wait(80);
    expect(errors).toContain('rate_limited');
  });

  it('closes connections that send oversized payloads', async () => {
    await start({ maxPayloadBytes: 1024 });
    const c = mk({ reconnect: false });
    await open(c);
    c.publish('x', 'a'.repeat(5000));
    await new Promise((r) => c.onStatus((s) => s === 'closed' && r(s)));
  });
});
