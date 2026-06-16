import { describe, it, expect, beforeEach, afterEach, mock, jest } from 'bun:test';
import { SSECoordinator } from '../src/coordinator';

/**
 * Shared BroadcastChannel mock that routes messages between coordinator instances
 * within the same test, simulating real multi-tab behaviour.
 */
const channelRegistry = new Map<string, Set<(e: MessageEvent) => void>>();

class SharedBroadcastChannel {
  private ownListeners: Array<(e: MessageEvent) => void> = [];

  constructor(private name: string) {
    if (!channelRegistry.has(name)) channelRegistry.set(name, new Set());
  }

  addEventListener(type: string, handler: (e: MessageEvent) => void) {
    if (type !== 'message') return;
    this.ownListeners.push(handler);
    channelRegistry.get(this.name)!.add(handler);
  }

  removeEventListener(type: string, handler: (e: MessageEvent) => void) {
    if (type !== 'message') return;
    const idx = this.ownListeners.indexOf(handler);
    if (idx !== -1) this.ownListeners.splice(idx, 1);
    channelRegistry.get(this.name)!.delete(handler);
  }

  postMessage(data: unknown) {
    const event = new MessageEvent('message', { data });
    for (const listener of [...(channelRegistry.get(this.name) ?? [])]) {
      if (!this.ownListeners.includes(listener)) {
        listener(event);
      }
    }
  }

  close() {
    for (const listener of this.ownListeners) {
      channelRegistry.get(this.name)?.delete(listener);
    }
    this.ownListeners = [];
  }
}

/**
 * Web Locks mock that serialises lock requests per name.
 * Mimics the browser: first requestor runs immediately, others queue.
 * When the holder's async callback resolves (lock released), the next in
 * queue runs automatically.
 */
function createLocksMock() {
  const held = new Map<string, boolean>();
  const queues = new Map<string, Array<() => void>>();

  const runNext = (name: string) => {
    const q = queues.get(name) ?? [];
    const next = q.shift();
    if (next) next();
  };

  const request = (_name: string, optionsOrCallback: any, maybeCallback?: any): Promise<void> => {
    const hasOptions = typeof optionsOrCallback === 'object' && optionsOrCallback !== null;
    const options = hasOptions ? optionsOrCallback : {};
    const callback = hasOptions ? maybeCallback : optionsOrCallback;
    const signal: AbortSignal | undefined = options.signal;

    return new Promise<void>((resolve, reject) => {
      if (signal?.aborted) { reject(new DOMException('Aborted', 'AbortError')); return; }

      const run = async () => {
        if (signal?.aborted) { reject(new DOMException('Aborted', 'AbortError')); runNext(_name); return; }
        held.set(_name, true);
        try { await callback({}); resolve(); }
        catch (e) { reject(e); }
        finally { held.set(_name, false); runNext(_name); }
      };

      if (signal) {
        signal.addEventListener('abort', () => {
          const q = queues.get(_name);
          if (q) {
            const idx = q.indexOf(run);
            if (idx >= 0) { q.splice(idx, 1); reject(new DOMException('Aborted', 'AbortError')); }
          }
        }, { once: true });
      }

      if (!held.get(_name)) { run(); }
      else { if (!queues.has(_name)) queues.set(_name, []); queues.get(_name)!.push(run); }
    });
  };

  return { request };
}

const CHANNEL = 'test-channel';
const TEST_URL = 'https://api.example.com/events';
const TEST_EVENTS = ['message'];

function makeCoordinator(): SSECoordinator {
  const c = new SSECoordinator();
  c.connect({ url: TEST_URL, eventTypes: TEST_EVENTS, channelName: CHANNEL, onEvent: () => {} });
  return c;
}

beforeEach(() => {
  jest.useFakeTimers();
  channelRegistry.clear();

  globalThis.EventSource = class {
    addEventListener() {}
    removeEventListener() {}
    close() {}
    onopen = null;
    onerror = null;
  } as any;

  globalThis.BroadcastChannel = SharedBroadcastChannel as any;
  (globalThis as any).navigator = { locks: createLocksMock() };
});

afterEach(() => {
  jest.useRealTimers();
  channelRegistry.clear();
});

describe('Multi-tab leader election', () => {
  it('elects exactly one leader when three tabs connect simultaneously', () => {
    // Web Locks gives the lock to the first requestor immediately;
    // the other two queue. No timers needed — election is instant.
    const a = makeCoordinator();
    const b = makeCoordinator();
    const c = makeCoordinator();

    const leaders = [a, b, c].filter(x => x.isLeader());
    expect(leaders).toHaveLength(1);

    [a, b, c].forEach(x => x.disconnect());
  });

  it('elects exactly one new leader among two followers when the leader disconnects', async () => {
    const a = makeCoordinator();
    const b = makeCoordinator();
    const c = makeCoordinator();

    expect([a, b, c].filter(x => x.isLeader())).toHaveLength(1);

    const [leader] = [a, b, c].filter(x => x.isLeader());
    const followers = [a, b, c].filter(x => !x.isLeader());

    leader.disconnect();

    // The promise chain (releaseLock → runAsLeader resolves → callback resolves →
    // run() finally → runNext → next callback starts) requires several microtasks.
    for (let i = 0; i < 5; i++) await Promise.resolve();

    const newLeaders = followers.filter(x => x.isLeader());
    expect(newLeaders).toHaveLength(1);

    followers.forEach(x => x.disconnect());
  });

  it('followers stay as followers while the leader holds the lock', () => {
    const a = makeCoordinator();
    const b = makeCoordinator();
    const c = makeCoordinator();

    // No amount of time passing changes the lock holder.
    jest.advanceTimersByTime(60_000);

    expect([a, b, c].filter(x => x.isLeader())).toHaveLength(1);
    expect([a, b, c].filter(x => !x.isLeader())).toHaveLength(2);

    [a, b, c].forEach(x => x.disconnect());
  });

  it('fires onConnectionChange(true) on the promoted tab after leader failover', async () => {
    let createdSources: any[] = [];

    class TrackingEventSource {
      onopen: ((e: Event) => void) | null = null;
      onerror: ((e: Event) => void) | null = null;
      constructor() { createdSources.push(this); }
      addEventListener() {}
      removeEventListener() {}
      close() {}
      fireOpen() { this.onopen?.(new Event('open')); }
    }
    globalThis.EventSource = TrackingEventSource as any;

    const onChangePrimary = mock(() => {});
    const onChangeFollower = mock(() => {});

    const a = new SSECoordinator();
    a.connect({ url: TEST_URL, eventTypes: TEST_EVENTS, channelName: CHANNEL, onEvent: () => {}, onConnectionChange: onChangePrimary });
    const b = new SSECoordinator();
    b.connect({ url: TEST_URL, eventTypes: TEST_EVENTS, channelName: CHANNEL, onEvent: () => {}, onConnectionChange: onChangeFollower });

    expect(a.isLeader()).toBe(true);
    expect(b.isLeader()).toBe(false);
    expect(createdSources).toHaveLength(1);

    createdSources[0].fireOpen();
    expect(onChangePrimary).toHaveBeenCalledWith(true);
    // Follower receives connection-state relay via BroadcastChannel
    expect(onChangeFollower).toHaveBeenCalledWith(true);

    onChangePrimary.mockClear();
    onChangeFollower.mockClear();

    a.disconnect();
    for (let i = 0; i < 5; i++) await Promise.resolve(); // let b acquire the lock

    expect(b.isLeader()).toBe(true);
    expect(createdSources).toHaveLength(2);
    createdSources[1].fireOpen();
    expect(onChangeFollower).toHaveBeenCalledWith(true);

    b.disconnect();
  });

  it('a late-joining tab stays a follower when a leader is already holding the lock', () => {
    const a = makeCoordinator();
    expect(a.isLeader()).toBe(true);

    jest.advanceTimersByTime(10_000);

    const lateB = makeCoordinator();
    expect(lateB.isLeader()).toBe(false);

    [a, lateB].forEach(x => x.disconnect());
  });

  it('follower receives connection-state relay from leader via BroadcastChannel', () => {
    const followerOnChange = mock(() => {});

    const a = makeCoordinator();
    const b = new SSECoordinator();
    b.connect({ url: TEST_URL, eventTypes: TEST_EVENTS, channelName: CHANNEL, onEvent: () => {}, onConnectionChange: followerOnChange });

    expect(a.isLeader()).toBe(true);
    expect(b.isLeader()).toBe(false);

    // Simulate leader's EventSource opening and broadcasting state
    b.handleBroadcastMessage({ type: 'connection-state', tabId: (a as any).tabId, connected: true } as any);

    expect(followerOnChange).toHaveBeenCalledWith(true);

    [a, b].forEach(x => x.disconnect());
  });
});
