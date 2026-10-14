import {
  Global,
  Inject,
  Injectable,
  Module,
  type DynamicModule,
  type OnApplicationBootstrap,
  type OnApplicationShutdown,
} from '@nestjs/common';
import { HttpAdapterHost } from '@nestjs/core';
import {
  createRealtimeServer,
  RealtimeError,
  type ClientContext,
  type RealtimeServer,
  type RoomHandle,
  type RealtimeServerOptions,
} from '../server';

export { RealtimeError };
export type { ClientContext, RealtimeServerOptions };

export const REALTIME_OPTIONS = Symbol('REALTIME_OPTIONS');

/** Injectable wrapper around the realtime server, attached to Nest's HTTP server on bootstrap. */
@Injectable()
export class RealtimeService<User = unknown> implements OnApplicationBootstrap, OnApplicationShutdown {
  private rt?: RealtimeServer<User>;
  // Providers usually register in onModuleInit, which runs before the HTTP server is available.
  private queued: Array<(rt: RealtimeServer<User>) => void> = [];

  constructor(
    @Inject(REALTIME_OPTIONS) private readonly options: Omit<RealtimeServerOptions<User>, 'server' | 'port'>,
    @Inject(HttpAdapterHost) private readonly adapterHost: HttpAdapterHost,
  ) {}

  onApplicationBootstrap(): void {
    this.rt = createRealtimeServer<User>({ ...this.options, server: this.adapterHost.httpAdapter.getHttpServer() });
    for (const fn of this.queued.splice(0)) fn(this.rt);
  }

  async onApplicationShutdown(): Promise<void> {
    await this.rt?.close();
  }

  private run(fn: (rt: RealtimeServer<User>) => void): void {
    if (this.rt) fn(this.rt); else this.queued.push(fn);
  }

  /** Publish to a channel. Returns recipients (0 if the server has not started yet). */
  publish(channel: string, data?: unknown, opts?: { except?: string }): number {
    return this.rt?.publish(channel, data, opts) ?? 0;
  }

  onRequest(method: string, handler: Parameters<RealtimeServer<User>['onRequest']>[1]): void {
    this.run((rt) => rt.onRequest(method, handler));
  }

  onConnection(cb: (ctx: ClientContext<User>) => void): void {
    this.run((rt) => rt.onConnection(cb));
  }

  onDisconnect(cb: (ctx: ClientContext<User>) => void): void {
    this.run((rt) => rt.onDisconnect(cb));
  }

  disconnect(clientId: string, code?: number, reason?: string): void {
    this.rt?.disconnect(clientId, code, reason);
  }

  /** Close all connections of a user (logout, ban). */
  disconnectUser(userId: string | number, code?: number, reason?: string): void {
    this.rt?.disconnectUser(userId, code, reason);
  }

  /** Direct message / notification to every tab and device of a user. Returns local deliveries. */
  sendToUser(userId: string | number, data?: unknown, opts?: { from?: string }): number {
    return this.rt?.sendToUser(userId, data, opts) ?? 0;
  }

  /** Server-side access to a room (broadcast from your own code, list members). */
  room(name: string): RoomHandle {
    const svc = this;
    return {
      send: (data) => this.rt?.room(name).send(data) ?? 0,
      members: () => this.rt?.room(name).members() ?? [],
      get size() { return svc.rt?.room(name).size ?? 0; },
    };
  }

  isOnline(userId: string | number): boolean {
    return this.rt?.isOnline(userId) ?? false;
  }

  onlineUsers(): string[] {
    return this.rt?.onlineUsers() ?? [];
  }

  get size(): number {
    return this.rt?.size ?? 0;
  }
}

export interface RealtimeModuleAsyncOptions<User> {
  imports?: any[];
  inject?: any[];
  useFactory: (...args: any[]) => Omit<RealtimeServerOptions<User>, 'server' | 'port'> | Promise<Omit<RealtimeServerOptions<User>, 'server' | 'port'>>;
}

@Global()
@Module({})
export class RealtimeModule {
  static forRoot<User = unknown>(options: Omit<RealtimeServerOptions<User>, 'server' | 'port'> = {}): DynamicModule {
    return {
      module: RealtimeModule,
      providers: [{ provide: REALTIME_OPTIONS, useValue: options }, RealtimeService],
      exports: [RealtimeService],
    };
  }

  /** Use when options need ConfigService, e.g. allowedOrigins from env. */
  static forRootAsync<User = unknown>(opts: RealtimeModuleAsyncOptions<User>): DynamicModule {
    return {
      module: RealtimeModule,
      imports: opts.imports ?? [],
      providers: [{ provide: REALTIME_OPTIONS, useFactory: opts.useFactory, inject: opts.inject ?? [] }, RealtimeService],
      exports: [RealtimeService],
    };
  }
}
