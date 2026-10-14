import { Redis, type RedisOptions } from 'ioredis';
import type { ClusterAdapter } from '../shared/protocol';

export interface RedisAdapterOptions {
  /** Connection string, e.g. "redis://user:password@host:6379/0" or "rediss://..." for TLS. */
  url?: string;
  /** ioredis options (host, port, username, password, db, tls, ...). Merged with `url` if both are given. */
  options?: RedisOptions;
  /**
   * An existing ioredis client to reuse for publishing. A second connection is created for
   * subscribing (a subscribed Redis connection cannot publish). The adapter will not close your client.
   */
  client?: Redis;
  /** Pub/Sub channel used between instances. Use a different one per app/environment. Default "realtime-kit". */
  channel?: string;
  /** Called on Redis connection errors (ioredis reconnects automatically). Default: console.error. */
  onError?: (error: unknown) => void;
}

/**
 * Redis-backed cluster adapter. Pass it to `createRealtimeServer({ adapter })` so messages
 * published on one server instance reach clients connected to every other instance.
 *
 * Redis is used only for message fan-out; nothing is stored in it.
 */
export function createRedisAdapter(opts: RedisAdapterOptions = {}): ClusterAdapter {
  const channel = opts.channel ?? 'realtime-kit';
  const onError = opts.onError ?? ((e) => console.error('[realtime-kit/redis]', e));
  const ownsPub = !opts.client;

  const pub =
    opts.client ??
    (opts.url ? new Redis(opts.url, opts.options ?? {}) : new Redis(opts.options ?? {}));
  const sub = pub.duplicate();
  pub.on('error', onError);
  sub.on('error', onError);

  return {
    async publish(message) {
      await pub.publish(channel, message);
    },
    async subscribe(handler) {
      sub.on('message', (ch, message) => {
        if (ch === channel) handler(message);
      });
      await sub.subscribe(channel); // ioredis re-subscribes automatically after reconnects
    },
    async close() {
      sub.disconnect();
      if (ownsPub) pub.disconnect();
      else pub.off('error', onError);
    },
  };
}
