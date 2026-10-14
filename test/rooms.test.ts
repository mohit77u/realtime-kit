import { createServer, type Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import { afterEach, describe, expect, it } from 'vitest';
import WebSocket from 'ws';
import { createRealtimeServer, RealtimeError, type RealtimeServer, type RealtimeServerOptions } from '../src/server';
import { RealtimeClient, type DirectMessage, type PresenceEvent, type RoomMessage } from '../src/client';
import { createRedisAdapter } from '../src/redis';

type U = { id: string };
const wait = (ms: number) => new Promise((r) => setTimeout(r, ms));

interface Node { http: Server; rt: RealtimeServer<U>; url: string }
const nodes: Node[] = [];
const clients: RealtimeClient[] = [];

async function start(opts: Partial<RealtimeServerOptions<U>> = {}): Promise<Node> {
  const http = createServer();
  const rt = createRealtimeServer<U>({
    server: http,
    authenticate: (_r, token) => (token ? { id: token } : null),
    userId: (u) => u.id,
    rooms: { canJoin: () => true },
    ...opts,
  });
  await new Promise<void>((r) => http.listen(0, r));
  const node = { http, rt, url: `ws://127.0.0.1:${(http.address() as AddressInfo).port}/realtime` };
  nodes.push(node);
  return node;
}

async function connect(node: Node, user: string, extra: Record<string, unknown> = {}) {
  const c = new RealtimeClient({ url: node.url, WebSocket: WebSocket as any, auth: () => user, minDelayMs: 20, maxDelayMs: 40, ...extra });
  clients.push(c);
  await new Promise<void>((res) => (c.status === 'open' ? res() : void c.onStatus((s) => s === 'open' && res())));
  return c;
}

afterEach(async () => {
  clients.splice(0).forEach((c) => c.close());
  for (const n of nodes.splice(0)) {
    await n.rt.close();
    await new Promise<void>((r) => n.http.close(() => r()));
  }
});

describe('rooms', () => {
  it('denies joining by default (no canJoin)', async () => {
    const n = await start({ rooms: {} });
    const a = await connect(n, 'ann');
    await expect(a.room('r1').join()).rejects.toMatchObject({ code: 'forbidden' });
  });

  it('delivers messages to all members with a server-set sender', async () => {
    const n = await start();
    const [a, b] = [await connect(n, 'ann'), await connect(n, 'bob')];
    const got: RoomMessage<string>[] = [];
    b.room<string>('r1').onMessage((m) => got.push(m));
    await a.room('r1').join();
    await b.room('r1').join();
    await a.room<string>('r1').send('hi');
    await wait(50);
    expect(got).toHaveLength(1);
    expect(got[0]).toMatchObject({ room: 'r1', from: 'ann', data: 'hi' });
    expect(got[0]!.id).toBeTruthy();
  });

  it('blocks non-members from sending, reading members or history', async () => {
    const n = await start({ rooms: { canJoin: () => true, history: () => [] } });
    const a = await connect(n, 'ann');
    await expect(a.room('r1').send('x')).rejects.toMatchObject({ code: 'forbidden' });
    await expect(a.room('r1').members()).rejects.toMatchObject({ code: 'forbidden' });
    await expect(a.room('r1').history()).rejects.toMatchObject({ code: 'forbidden' });
  });

  it('enforces canJoin per room', async () => {
    const n = await start({ rooms: { canJoin: (ctx, room) => room === `private-${ctx.userId}` || room.startsWith('public-') } });
    const a = await connect(n, 'ann');
    await a.room('public-1').join();
    await a.room('private-ann').join();
    await expect(a.room('private-bob').join()).rejects.toMatchObject({ code: 'forbidden' });
  });

  it('reports presence and de-duplicates multiple tabs of one user', async () => {
    const n = await start();
    const watcher = await connect(n, 'watch');
    await watcher.room('r1').join();
    const events: PresenceEvent[] = [];
    watcher.room('r1').onPresence((e) => events.push(e));

    const tab1 = await connect(n, 'ann');
    const tab2 = await connect(n, 'ann');
    await tab1.room('r1').join();
    await tab2.room('r1').join();
    await wait(50);
    expect(events.map((e) => `${e.userId}:${e.online}`)).toEqual(['ann:true']); // one join for two tabs

    expect((await watcher.room('r1').members()).sort()).toEqual(['ann', 'watch']);
    tab1.close();
    await wait(50);
    expect(events).toHaveLength(1); // still online via tab2
    tab2.close();
    await wait(50);
    expect(events.map((e) => `${e.userId}:${e.online}`)).toEqual(['ann:true', 'ann:false']);
  });

  it('runs onMessage hook: can transform and reject', async () => {
    const n = await start({
      rooms: {
        canJoin: () => true,
        onMessage: (_c, _room, data: any) => {
          if (String(data).includes('spam')) throw new RealtimeError('bad_request', 'no spam');
          return { text: String(data).trim(), saved: true };
        },
      },
    });
    const a = await connect(n, 'ann');
    const got: RoomMessage<any>[] = [];
    a.room('r1').onMessage((m) => got.push(m));
    await a.room('r1').join();
    await a.room('r1').send('  hello ');
    await expect(a.room('r1').send('spam!')).rejects.toMatchObject({ code: 'bad_request' });
    await wait(50);
    expect(got.map((m) => m.data)).toEqual([{ text: 'hello', saved: true }]);
  });

  it('serves history through the hook with a clamped limit', async () => {
    let seen: any;
    const n = await start({ rooms: { canJoin: () => true, history: (_c, room, q) => { seen = q; return [{ room, n: 1 }]; } } });
    const a = await connect(n, 'ann');
    await a.room('r1').join();
    expect(await a.room('r1').history({ before: 'm9', limit: 5000 })).toEqual([{ room: 'r1', n: 1 }]);
    expect(seen).toEqual({ before: 'm9', limit: 100 });
  });

  it('rejoins rooms automatically after a reconnect', async () => {
    const n = await start();
    const a = await connect(n, 'ann');
    const b = await connect(n, 'bob');
    const got: unknown[] = [];
    a.room('r1').onMessage((m) => got.push(m.data));
    await a.room('r1').join();
    await b.room('r1').join();
    // kick ann (all her connections) and wait for the client to come back
    const reopened = new Promise<void>((r) => { const off = a.onStatus((s) => { if (s === 'open') { off(); r(); } }); });
    n.rt.disconnectUser('ann');
    await reopened;
    await wait(80);
    await b.room('r1').send('after reconnect');
    await wait(50);
    expect(got).toEqual(['after reconnect']);
  });

  it('keeps the room until every join() is matched by leave()', async () => {
    const n = await start();
    const a = await connect(n, 'ann');
    const room = a.room('r1');
    await room.join();
    await room.join();
    await room.leave();
    expect(room.joined).toBe(true);
    await room.leave();
    expect(room.joined).toBe(false);
    await expect(room.send('x')).rejects.toMatchObject({ code: 'forbidden' });
  });

  it('lets server code broadcast into a room', async () => {
    const n = await start();
    const a = await connect(n, 'ann');
    const got: RoomMessage[] = [];
    a.room('r1').onMessage((m) => got.push(m));
    await a.room('r1').join();
    expect(n.rt.room('r1').send({ system: true })).toBe(1);
    expect(n.rt.room('r1').members()).toEqual(['ann']);
    await wait(50);
    expect(got[0]).toMatchObject({ from: null, data: { system: true } });
  });

  it('refuses rooms for anonymous connections', async () => {
    const n = await start({ authenticate: undefined, userId: undefined });
    const a = new RealtimeClient({ url: n.url, WebSocket: WebSocket as any });
    clients.push(a);
    await new Promise<void>((res) => void a.onStatus((s) => s === 'open' && res()));
    await expect(a.room('r1').join()).rejects.toMatchObject({ code: 'forbidden' });
  });
});

describe('direct messages', () => {
  it('delivers to every tab of the recipient and echoes to the sender tabs', async () => {
    const n = await start();
    const a1 = await connect(n, 'ann');
    const a2 = await connect(n, 'ann');
    const b1 = await connect(n, 'bob');
    const b2 = await connect(n, 'bob');
    const got: Record<string, DirectMessage[]> = { a1: [], a2: [], b1: [], b2: [] };
    a1.onDirect((m) => got.a1!.push(m)); a2.onDirect((m) => got.a2!.push(m));
    b1.onDirect((m) => got.b1!.push(m)); b2.onDirect((m) => got.b2!.push(m));

    const res = await a1.sendToUser('bob', { text: 'hey' });
    await wait(50);
    expect(res.delivered).toBe(true);
    for (const k of ['a1', 'a2', 'b1', 'b2']) {
      expect(got[k]).toHaveLength(1);
      expect(got[k]![0]).toMatchObject({ from: 'ann', to: 'bob', data: { text: 'hey' } });
    }
  });

  it('reports delivered=false for offline users and supports canSend', async () => {
    const n = await start({ direct: { canSend: (_c, to) => to !== 'blocked' } });
    const a = await connect(n, 'ann');
    expect((await a.sendToUser('nobody', 1)).delivered).toBe(false);
    await expect(a.sendToUser('blocked', 1)).rejects.toMatchObject({ code: 'forbidden' });
  });

  it('runs the onMessage hook and serves history', async () => {
    const stored: unknown[] = [];
    const n = await start({
      direct: {
        onMessage: (ctx, to, data) => { const m = { from: ctx.userId, to, data }; stored.push(m); return data; },
        history: (_c, withUser) => stored.filter((m: any) => m.to === withUser || m.from === withUser),
      },
    });
    const a = await connect(n, 'ann');
    await a.sendToUser('bob', 'one');
    expect(stored).toHaveLength(1);
    expect(await a.directHistory('bob')).toHaveLength(1);
  });

  it('lets server code notify a user and track presence', async () => {
    const n = await start();
    const a = await connect(n, 'ann');
    const got: DirectMessage[] = [];
    a.onDirect((m) => got.push(m));
    expect(n.rt.isOnline('ann')).toBe(true);
    expect(n.rt.isOnline('zed')).toBe(false);
    expect(n.rt.onlineUsers()).toEqual(['ann']);
    expect(n.rt.sendToUser('ann', { order: 5 })).toBe(1);
    await wait(50);
    expect(got[0]).toMatchObject({ from: null, to: 'ann', data: { order: 5 } });
  });

  it('reserves the rk. prefix for built-in methods', async () => {
    const n = await start();
    expect(() => n.rt.onRequest('rk.room.send', () => 1)).toThrow(/reserved/);
  });
});

describe('redis cluster adapter (two server instances, real Redis)', () => {
  const channel = `rk-test-${Math.random().toString(36).slice(2)}`;
  const adapter = () => createRedisAdapter({ url: process.env.REDIS_URL ?? 'redis://127.0.0.1:6379', channel });

  it('fans out channels, rooms and direct messages across instances', async () => {
    const n1 = await start({ adapter: adapter(), authorize: () => true });
    const n2 = await start({ adapter: adapter(), authorize: () => true });
    await wait(150); // let Redis subscriptions settle
    const a = await connect(n1, 'ann');
    const b = await connect(n2, 'bob');

    const chan: unknown[] = []; const room: unknown[] = []; const dm: unknown[] = [];
    b.subscribe('news', (d) => chan.push(d));
    b.room('r1').onMessage((m) => room.push([m.from, m.data]));
    b.onDirect((m) => dm.push([m.from, m.data]));
    await b.room('r1').join();
    await a.room('r1').join();
    await wait(80);

    n1.rt.publish('news', 'from-n1');
    await a.room('r1').send('hello across');
    await a.sendToUser('bob', 'dm across');
    n1.rt.sendToUser('bob', 'server dm');
    await wait(200);

    expect(chan).toEqual(['from-n1']);
    expect(room).toEqual([['ann', 'hello across']]);
    expect(dm).toEqual([['ann', 'dm across'], [null, 'server dm']]);
  });

  it('does not double-deliver on the originating instance', async () => {
    const n1 = await start({ adapter: adapter(), authorize: () => true });
    await start({ adapter: adapter(), authorize: () => true });
    await wait(150);
    const a = await connect(n1, 'ann');
    const got: unknown[] = [];
    a.subscribe('news', (d) => got.push(d));
    await wait(50);
    n1.rt.publish('news', 'once');
    await wait(200);
    expect(got).toEqual(['once']);
  });

  it('shares members, presence events and isOnline across instances', async () => {
    const n1 = await start({ adapter: adapter(), clusterSyncMs: 100 });
    const n2 = await start({ adapter: adapter(), clusterSyncMs: 100 });
    await wait(200);
    const ann = await connect(n1, 'ann');
    await ann.room('r1').join();
    const events: string[] = [];
    ann.room('r1').onPresence((e) => events.push(`${e.userId}:${e.online}`));
    await wait(100);

    const bob = await connect(n2, 'bob');
    expect((await bob.room('r1').join()).sort()).toEqual(['ann', 'bob']); // bob (instance 2) sees ann (instance 1)
    await wait(150);
    expect(events).toEqual(['bob:true']); // ann is told about bob exactly once
    expect(n1.rt.isOnline('bob')).toBe(true);
    expect(n2.rt.isOnline('ann')).toBe(true);
    expect(n1.rt.onlineUsers().sort()).toEqual(['ann', 'bob']);
    expect(n1.rt.room('r1').members().sort()).toEqual(['ann', 'bob']);

    // same user on both instances counts once and stays "in" until the last tab leaves
    const bob2 = await connect(n1, 'bob');
    await bob2.room('r1').join();
    await wait(150);
    expect(events).toEqual(['bob:true']);
    bob.close();
    await wait(200);
    expect(events).toEqual(['bob:true']); // still present via instance 1
    bob2.close();
    await wait(200);
    expect(events).toEqual(['bob:true', 'bob:false']);
    expect(n1.rt.isOnline('bob')).toBe(false);
  });

  it('catches a late-starting instance up on existing members (hello/state)', async () => {
    const n1 = await start({ adapter: adapter(), clusterSyncMs: 100 });
    await wait(150);
    const ann = await connect(n1, 'ann');
    await ann.room('r1').join();
    const n2 = await start({ adapter: adapter(), clusterSyncMs: 100 });
    await wait(300);
    const bob = await connect(n2, 'bob');
    expect((await bob.room('r1').join()).sort()).toEqual(['ann', 'bob']);
  });

  it('drops members of an instance that shuts down (bye) or disappears (heartbeat timeout)', async () => {
    const n1 = await start({ adapter: adapter(), clusterSyncMs: 100 });
    const n2 = await start({ adapter: adapter(), clusterSyncMs: 100 });
    await wait(200);
    const ann = await connect(n1, 'ann');
    await ann.room('r1').join();
    const bob = await connect(n2, 'bob');
    const events: string[] = [];
    bob.room('r1').onPresence((e) => events.push(`${e.userId}:${e.online}`));
    await bob.room('r1').join();
    await wait(200);

    // graceful: n1 shuts down -> bye -> ann leaves for bob
    await n1.rt.close();
    await wait(200);
    expect(events).toContain('ann:false');
    expect(n2.rt.room('r1').members()).toEqual(['bob']);
  });

  it('removes a crashed instance after the heartbeat timeout', async () => {
    const inner = adapter();
    let crashed = false;
    const flaky = { ...inner, publish: (m: string) => (crashed ? undefined : inner.publish(m)) }; // a dead process publishes nothing
    const n1 = await start({ adapter: flaky, clusterSyncMs: 80 });
    const n2 = await start({ adapter: adapter(), clusterSyncMs: 80 });
    await wait(200);
    const ann = await connect(n1, 'ann');
    await ann.room('r1').join();
    const bob = await connect(n2, 'bob');
    const events: string[] = [];
    bob.room('r1').onPresence((e) => events.push(`${e.userId}:${e.online}`));
    await bob.room('r1').join();
    await wait(150);
    expect(n2.rt.isOnline('ann')).toBe(true);

    crashed = true; // no more heartbeats from n1, and no "bye"
    await wait(500);
    expect(n2.rt.isOnline('ann')).toBe(false);
    expect(events).toContain('ann:false');
    expect(n2.rt.room('r1').members()).toEqual(['bob']);
  });
});
