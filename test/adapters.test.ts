import 'reflect-metadata';
import { createServer } from 'node:http';
import type { AddressInfo } from 'node:net';
import { effectScope, nextTick, ref } from 'vue';
import { firstValueFrom } from 'rxjs';
import { take, toArray } from 'rxjs/operators';
import { Test } from '@nestjs/testing';
import { Inject, Injectable, Module, type INestApplication } from '@nestjs/common';
import WebSocket from 'ws';
import { afterEach, describe, expect, it } from 'vitest';
import { createRealtimeServer } from '../src/server';
import { RealtimeClient } from '../src/client';
import { useChannel as useVueChannel, useConnectionStatus, useDirect as useVueDirect, useRoom as useVueRoom } from '../src/vue';
import { channel$, direct$, members$, request$, room$, status$ } from '../src/angular';
import { RealtimeModule, RealtimeService, RealtimeError } from '../src/nest';

const wait = (ms: number) => new Promise((r) => setTimeout(r, ms));
const cleanup: Array<() => unknown> = [];
afterEach(async () => { for (const fn of cleanup.splice(0)) await fn(); });

async function plain() {
  const http = createServer();
  const rt = createRealtimeServer({ server: http, userId: (u: any) => u.id, authenticate: () => ({ id: 'me' }), rooms: { canJoin: () => true } });
  await new Promise<void>((r) => http.listen(0, r));
  const url = `ws://127.0.0.1:${(http.address() as AddressInfo).port}/realtime`;
  const client = new RealtimeClient({ url, WebSocket: WebSocket as any });
  cleanup.push(() => client.close(), () => rt.close(), () => new Promise((r) => http.close(r)));
  await new Promise<void>((r) => (client.status === 'open' ? r() : void client.onStatus((s) => s === 'open' && r())));
  return { rt, client };
}

describe('vue', () => {
  it('receives messages, follows a ref channel and unsubscribes on scope stop', async () => {
    const { rt, client } = await plain();
    const got: unknown[] = [];
    const channel = ref('a');
    const scope = effectScope();
    scope.run(() => { useVueChannel(client, channel, (d) => got.push(d)); });
    await wait(40);
    rt.publish('a', 1);
    await wait(40);
    channel.value = 'b';
    await nextTick(); await wait(40);
    rt.publish('a', 'ignored'); rt.publish('b', 2);
    await wait(40);
    scope.stop();
    await wait(40);
    expect(rt.publish('b', 3)).toBe(0);
    expect(got).toEqual([1, 2]);
  });

  it('exposes reactive status', async () => {
    const { client } = await plain();
    const scope = effectScope();
    const status = scope.run(() => useConnectionStatus(client))!;
    expect(status.value).toBe('open');
    client.close();
    expect(status.value).toBe('closed');
    scope.stop();
  });
});

describe('angular (rxjs)', () => {
  it('channel$, request$ and status$', async () => {
    const { rt, client } = await plain();
    rt.onRequest('double', (n: any) => n * 2);
    const msgs = firstValueFrom(channel$<number>(client, 'n').pipe(take(2), toArray()));
    await wait(40);
    rt.publish('n', 1); rt.publish('n', 2);
    expect(await msgs).toEqual([1, 2]);
    expect(await firstValueFrom(request$<number>(client, 'double', 21))).toBe(42);
    expect(await firstValueFrom(status$(client))).toBe('open');
    await wait(40);
    expect(rt.publish('n', 3)).toBe(0); // unsubscribed after take(2)
  });
});

describe('rooms in vue / rxjs', () => {
  it('vue useRoom joins, receives, tracks members, leaves on scope stop; useDirect receives', async () => {
    const { rt, client } = await plain();
    const got: unknown[] = []; const dms: unknown[] = [];
    const scope = effectScope();
    const r = scope.run(() => { useVueDirect(client, (m) => dms.push(m.data)); return useVueRoom<string>(client, 'r1', (m) => got.push(m.data)); })!;
    await wait(80);
    expect(r.joined.value).toBe(true);
    expect(r.members.value).toEqual(['me']);
    await r.send('hello');
    rt.sendToUser('me', 'ping');
    await wait(60);
    expect(got).toEqual(['hello']);
    expect(dms).toEqual(['ping']);
    scope.stop();
    await wait(60);
    expect(rt.room('r1').size).toBe(0);
  });

  it('rxjs room$ / members$ / direct$ join on subscribe and leave on unsubscribe', async () => {
    const { rt, client } = await plain();
    const msgs = firstValueFrom(room$<string>(client, 'r2').pipe(take(1)));
    const members = firstValueFrom(members$(client, 'r2').pipe(take(1)));
    const dm = firstValueFrom(direct$<string>(client).pipe(take(1)));
    await wait(80);
    rt.room('r2').send('from server');
    rt.sendToUser('me', 'note');
    expect((await msgs).data).toBe('from server');
    expect(await members).toEqual(['me']);
    expect((await dm).data).toBe('note');
    await wait(60);
    expect(rt.room('r2').size).toBe(0); // both subscriptions ended -> left
  });
});

describe('nestjs', () => {
  it('attaches to the Nest http server and serves handlers registered in onModuleInit', async () => {
    @Injectable()
    class Handlers {
      constructor(@Inject(RealtimeService) rt: RealtimeService) {
        rt.onRequest('hello', (d: any) => `hi ${d.name}`);
        rt.onRequest('deny', () => { throw new RealtimeError('forbidden', 'nope'); });
      }
    }
    @Module({ imports: [RealtimeModule.forRoot({})], providers: [Handlers] })
    class AppModule {}

    const mod = await Test.createTestingModule({ imports: [AppModule] }).compile();
    const app: INestApplication = mod.createNestApplication();
    await app.init();
    await app.listen(0);
    cleanup.push(() => app.close());

    const url = `ws://127.0.0.1:${(app.getHttpServer().address() as AddressInfo).port}/realtime`;
    const client = new RealtimeClient({ url, WebSocket: WebSocket as any });
    cleanup.unshift(() => client.close());
    expect(await client.request('hello', { name: 'ann' })).toBe('hi ann');
    await expect(client.request('deny')).rejects.toMatchObject({ code: 'forbidden' });
    const svc = app.get(RealtimeService);
    expect(svc.size).toBe(1);
  });
});
