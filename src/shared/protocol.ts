/** Wire protocol shared by server and client. All frames are JSON text. */

export const SUBPROTOCOL = 'realtime-kit';
export const AUTH_PROTOCOL_PREFIX = 'auth.';

export type ClientFrame =
  | { type: 'subscribe'; channel: string }
  | { type: 'unsubscribe'; channel: string }
  | { type: 'publish'; channel: string; data?: unknown }
  | { type: 'request'; id: string; method: string; data?: unknown };

export type ServerFrame =
  | { type: 'ready' }
  | { type: 'message'; channel: string; data?: unknown }
  | { type: 'response'; id: string; data?: unknown }
  | { type: 'error'; id?: string; code: ErrorCode; message: string };

export type ErrorCode =
  | 'bad_request'
  | 'forbidden'
  | 'rate_limited'
  | 'not_found'
  | 'internal';

const CHANNEL_RE = /^[\w.:\-/]{1,128}$/;

export function isValidChannel(value: unknown): value is string {
  return typeof value === 'string' && CHANNEL_RE.test(value);
}

/** Parses and structurally validates an incoming client frame. Returns null if invalid. */
export function parseClientFrame(raw: string): ClientFrame | null {
  let f: any;
  try {
    f = JSON.parse(raw);
  } catch {
    return null;
  }
  if (!f || typeof f !== 'object') return null;
  switch (f.type) {
    case 'subscribe':
    case 'unsubscribe':
      return isValidChannel(f.channel) ? { type: f.type, channel: f.channel } : null;
    case 'publish':
      return isValidChannel(f.channel) ? { type: 'publish', channel: f.channel, data: f.data } : null;
    case 'request':
      return typeof f.id === 'string' && f.id.length <= 64 && typeof f.method === 'string' && f.method.length <= 128
        ? { type: 'request', id: f.id, method: f.method, data: f.data }
        : null;
    default:
      return null;
  }
}
