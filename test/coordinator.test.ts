import { describe, it, expect, beforeEach, afterEach, mock, jest } from 'bun:test';
import { SSECoordinator } from '../src/coordinator';
import type { SSEEvent } from '../src/types';

const TEST_URL = 'https://api.example.com/events/stream';
const TEST_EVENTS = ['message', 'notification.created', 'processing.started'];

/**
 * Creates a navigator.locks mock that serialises lock requests per name.
 * The first requestor runs immediately; subsequent requestors queue and run
 * when the current holder releases (by resolving/returning from its callback).
 */
function createLocksMock() {
  const held = new Map<string, boolean>();
  const queues = new Map<string, Array<() => void>>();

  const request = (_name: string, optionsOrCallback: any, maybeCallback?: any): Promise<void> => {
    const hasOptions = typeof optionsOrCallback === 'object' && optionsOrCallback !== null;
    const options = hasOptions ? optionsOrCallback : {};
    const callback = hasOptions ? maybeCallback : optionsOrCallback;
    const signal: AbortSignal | undefined = options.signal;

    return new Promise<void>((resolve, reject) => {
      if (signal?.aborted) {
        reject(new DOMException('Aborted', 'AbortError'));
        return;
      }

      const run = async () => {
        if (signal?.aborted) {
          reject(new DOMException('Aborted', 'AbortError'));
          runNext(_name);
          return;
        }
        held.set(_name, true);
        try {
          await callback({});
          resolve();
        } catch (e) {
          reject(e);
        } finally {
          held.set(_name, false);
          runNext(_name);
        }
      };

      if (signal) {
        signal.addEventListener('abort', () => {
          const q = queues.get(_name);
          if (q) {
            const idx = q.indexOf(run);
            if (idx >= 0) {
              q.splice(idx, 1);
              reject(new DOMException('Aborted', 'AbortError'));
            }
          }
        }, { once: true });
      }

      if (!held.get(_name)) {
        run();
      } else {
        if (!queues.has(_name)) queues.set(_name, []);
        queues.get(_name)!.push(run);
      }
    });
  };

  const runNext = (name: string) => {
    const q = queues.get(name) ?? [];
    const next = q.shift();
    if (next) next();
  };

  return { request };
}

describe('SSECoordinator', () => {
  let coordinator: SSECoordinator;
  let broadcastChannelMock: any;
  let messages: any[] = [];

  beforeEach(() => {
    jest.useFakeTimers();
    messages = [];

    globalThis.EventSource = class {
      addEventListener() {}
      removeEventListener() {}
      close() {}
      onopen = null;
      onerror = null;
    } as any;

    broadcastChannelMock = {
      postMessage: mock((msg: any) => { messages.push(msg); }),
      close: mock(() => {}),
      addEventListener: mock(() => {}),
      removeEventListener: mock(() => {}),
    };

    globalThis.BroadcastChannel = mock(() => broadcastChannelMock) as any;

    (globalThis as any).navigator = { locks: createLocksMock() };
  });

  afterEach(() => {
    coordinator?.disconnect();
    jest.useRealTimers();
  });

  describe('Leader Election', () => {
    it('elects first tab as leader immediately', () => {
      coordinator = new SSECoordinator();
      coordinator.connect({ url: TEST_URL, eventTypes: TEST_EVENTS, onEvent: () => {} });

      expect(coordinator.isLeader()).toBe(true);
    });

    it('stays follower when another tab holds the lock', async () => {
      const leader = new SSECoordinator();
      leader.connect({ url: TEST_URL, eventTypes: TEST_EVENTS, onEvent: () => {} });
      expect(leader.isLeader()).toBe(true);

      coordinator = new SSECoordinator();
      coordinator.connect({ url: TEST_URL, eventTypes: TEST_EVENTS, onEvent: () => {} });

      expect(coordinator.isLeader()).toBe(false);

      leader.disconnect();
    });
  });

  describe('Event Broadcasting', () => {
    it('broadcasts SSE events to all tabs via BroadcastChannel', () => {
      coordinator = new SSECoordinator();
      coordinator.connect({ url: TEST_URL, eventTypes: TEST_EVENTS, onEvent: () => {} });

      const event: SSEEvent = {
        type: 'test-event',
        data: { message: 'hello' },
        id: '123',
        timestamp: new Date().toISOString(),
      };

      coordinator.broadcastEvent(event);

      expect(broadcastChannelMock.postMessage).toHaveBeenCalledWith(
        expect.objectContaining({ type: 'sse-event', event })
      );
    });

    it('receives events from BroadcastChannel and calls onEvent', () => {
      const onEvent = mock(() => {});
      coordinator = new SSECoordinator();
      coordinator.connect({ url: TEST_URL, eventTypes: TEST_EVENTS, onEvent });

      const event: SSEEvent = {
        type: 'notification.created',
        data: { id: 1 },
        id: '456',
        timestamp: new Date().toISOString(),
      };

      coordinator.handleBroadcastMessage({ type: 'sse-event', tabId: 'other-tab', event });

      expect(onEvent).toHaveBeenCalledWith(event);
    });
  });

  describe('Leader Failover', () => {
    it('promotes follower to leader when leader releases the lock', async () => {
      const leader = new SSECoordinator();
      leader.connect({ url: TEST_URL, eventTypes: TEST_EVENTS, onEvent: () => {} });
      expect(leader.isLeader()).toBe(true);

      coordinator = new SSECoordinator();
      coordinator.connect({ url: TEST_URL, eventTypes: TEST_EVENTS, onEvent: () => {} });
      expect(coordinator.isLeader()).toBe(false);

      leader.disconnect();

      // The promise chain (releaseLock → runAsLeader resolves → callback resolves →
      // run() finally → runNext → next callback starts) requires several microtasks.
      for (let i = 0; i < 5; i++) await Promise.resolve();

      expect(coordinator.isLeader()).toBe(true);
    });
  });

  describe('Connection Management', () => {
    it('only creates EventSource connection when leader', () => {
      coordinator = new SSECoordinator();
      const createConnectionSpy = mock(() => {});
      (coordinator as any).createEventSource = createConnectionSpy;

      coordinator.connect({ url: TEST_URL, eventTypes: TEST_EVENTS, onEvent: () => {} });

      if (coordinator.isLeader()) {
        expect(createConnectionSpy).toHaveBeenCalled();
      } else {
        expect(createConnectionSpy).not.toHaveBeenCalled();
      }
    });

    it('closes EventSource when demoted from leader', () => {
      coordinator = new SSECoordinator();
      coordinator.connect({ url: TEST_URL, eventTypes: TEST_EVENTS, onEvent: () => {} });

      const closeConnectionSpy = mock(() => {});
      (coordinator as any).closeEventSource = closeConnectionSpy;

      if (coordinator.isLeader()) {
        coordinator.demoteToFollower();
        expect(closeConnectionSpy).toHaveBeenCalled();
      }
    });
  });

  describe('Custom Options', () => {
    it('uses custom channelName when provided', () => {
      coordinator = new SSECoordinator();
      coordinator.connect({
        url: TEST_URL,
        eventTypes: TEST_EVENTS,
        channelName: 'my-app-sse',
        onEvent: () => {},
      });

      expect(globalThis.BroadcastChannel).toHaveBeenCalledWith('my-app-sse');
    });

    it('uses default channelName when not provided', () => {
      coordinator = new SSECoordinator();
      coordinator.connect({ url: TEST_URL, eventTypes: TEST_EVENTS, onEvent: () => {} });

      expect(globalThis.BroadcastChannel).toHaveBeenCalledWith('sse-coordinator');
    });

    it('calls logger.info when promoted to leader', () => {
      const logger = {
        debug: mock(() => {}),
        info: mock(() => {}),
        warn: mock(() => {}),
        error: mock(() => {}),
      };

      coordinator = new SSECoordinator();
      coordinator.connect({ url: TEST_URL, eventTypes: TEST_EVENTS, logger, onEvent: () => {} });

      expect(logger.info.mock.calls.length).toBeGreaterThan(0);
    });
  });

  describe('URL Validation', () => {
    it('throws on a completely invalid URL when no window is available', () => {
      const originalWindow = globalThis.window;
      (globalThis as any).window = undefined;

      coordinator = new SSECoordinator();
      expect(() =>
        coordinator.connect({ url: 'not a url', eventTypes: TEST_EVENTS, onEvent: () => {} })
      ).toThrow('Invalid URL');

      (globalThis as any).window = originalWindow;
    });

    it('throws on a non-http/https URL', () => {
      coordinator = new SSECoordinator();
      expect(() =>
        coordinator.connect({ url: 'ftp://example.com/events', eventTypes: TEST_EVENTS, onEvent: () => {} })
      ).toThrow('http or https');
    });

    it('accepts an http URL', () => {
      coordinator = new SSECoordinator();
      expect(() =>
        coordinator.connect({ url: 'http://localhost:3000/events', eventTypes: TEST_EVENTS, onEvent: () => {} })
      ).not.toThrow();
    });

    it('accepts a relative URL when window is available', () => {
      (globalThis as any).window = { location: { href: 'https://app.example.com/dashboard' } };

      coordinator = new SSECoordinator();
      expect(() =>
        coordinator.connect({ url: '/api/v1/events/stream', eventTypes: TEST_EVENTS, onEvent: () => {} })
      ).not.toThrow();

      (globalThis as any).window = undefined;
    });
  });

  describe('Double connect', () => {
    it('closes the previous BroadcastChannel when connect() is called twice', () => {
      coordinator = new SSECoordinator();
      coordinator.connect({ url: TEST_URL, eventTypes: TEST_EVENTS, onEvent: () => {} });
      const firstChannel = broadcastChannelMock;

      const secondChannelMock = {
        postMessage: mock(() => {}),
        close: mock(() => {}),
        addEventListener: mock(() => {}),
        removeEventListener: mock(() => {}),
      };
      globalThis.BroadcastChannel = mock(() => secondChannelMock) as any;

      coordinator.connect({ url: TEST_URL, eventTypes: TEST_EVENTS, onEvent: () => {} });

      expect(firstChannel.close).toHaveBeenCalled();
    });
  });

  describe('BroadcastChannel message validation', () => {
    it('ignores messages with an unknown type', () => {
      const onEvent = mock(() => {});
      coordinator = new SSECoordinator();
      coordinator.connect({ url: TEST_URL, eventTypes: TEST_EVENTS, onEvent });

      coordinator.handleBroadcastMessage({ type: 'unknown-type' } as any);

      expect(onEvent).not.toHaveBeenCalled();
    });

    it('ignores non-object messages', () => {
      const onEvent = mock(() => {});
      coordinator = new SSECoordinator();
      coordinator.connect({ url: TEST_URL, eventTypes: TEST_EVENTS, onEvent });

      coordinator.handleBroadcastMessage(null as any);
      coordinator.handleBroadcastMessage('string' as any);

      expect(onEvent).not.toHaveBeenCalled();
    });

    it('ignores messages from own tab', () => {
      const onEvent = mock(() => {});
      coordinator = new SSECoordinator();
      coordinator.connect({ url: TEST_URL, eventTypes: TEST_EVENTS, onEvent });

      const ownTabId = (coordinator as any).tabId;
      const event: SSEEvent = { type: 'x', data: {}, id: '1', timestamp: '' };
      coordinator.handleBroadcastMessage({ type: 'sse-event', tabId: ownTabId, event });

      expect(onEvent).not.toHaveBeenCalled();
    });
  });

  describe('connection-state relay to followers', () => {
    it('calls onConnectionChange on follower when leader broadcasts connection-state', () => {
      const leaderOnChange = mock(() => {});
      const leader = new SSECoordinator();
      leader.connect({ url: TEST_URL, eventTypes: TEST_EVENTS, onEvent: () => {}, onConnectionChange: leaderOnChange });

      const followerOnChange = mock(() => {});
      coordinator = new SSECoordinator();
      coordinator.connect({ url: TEST_URL, eventTypes: TEST_EVENTS, onEvent: () => {}, onConnectionChange: followerOnChange });

      // Simulate leader broadcasting connection-state
      coordinator.handleBroadcastMessage({ type: 'connection-state', tabId: 'leader-tab', connected: true });

      expect(followerOnChange).toHaveBeenCalledWith(true);

      leader.disconnect();
    });

    it('does not call onConnectionChange on leader for connection-state messages', () => {
      const onChange = mock(() => {});
      coordinator = new SSECoordinator();
      coordinator.connect({ url: TEST_URL, eventTypes: TEST_EVENTS, onEvent: () => {}, onConnectionChange: onChange });
      onChange.mockClear();

      // Leader should ignore connection-state from others (it manages its own state)
      coordinator.handleBroadcastMessage({ type: 'connection-state', tabId: 'other-tab', connected: false });

      expect(onChange).not.toHaveBeenCalled();
    });
  });

  describe('Web Locks unavailable (standalone fallback)', () => {
    it('does not throw and becomes standalone leader when navigator.locks is missing', () => {
      (globalThis as any).navigator = {};
      coordinator = new SSECoordinator();

      expect(() =>
        coordinator.connect({ url: TEST_URL, eventTypes: TEST_EVENTS, onEvent: () => {} })
      ).not.toThrow();
      expect(coordinator.isLeader()).toBe(true);
    });

    it('does not throw when navigator itself is undefined', () => {
      const originalNavigator = (globalThis as any).navigator;
      (globalThis as any).navigator = undefined;
      coordinator = new SSECoordinator();

      expect(() =>
        coordinator.connect({ url: 'http://localhost/events', eventTypes: TEST_EVENTS, onEvent: () => {} })
      ).not.toThrow();
      expect(coordinator.isLeader()).toBe(true);

      (globalThis as any).navigator = originalNavigator;
    });

    it('opens its own EventSource in standalone mode', () => {
      (globalThis as any).navigator = {};
      coordinator = new SSECoordinator();
      const spy = mock(() => {});
      (coordinator as any).createEventSource = spy;

      coordinator.connect({ url: TEST_URL, eventTypes: TEST_EVENTS, onEvent: () => {} });

      expect(spy).toHaveBeenCalled();
    });

    it('warns that it is running standalone', () => {
      (globalThis as any).navigator = {};
      const logger = { debug: mock(() => {}), info: mock(() => {}), warn: mock(() => {}), error: mock(() => {}) };
      coordinator = new SSECoordinator();

      coordinator.connect({ url: TEST_URL, eventTypes: TEST_EVENTS, logger, onEvent: () => {} });

      expect(logger.warn.mock.calls.length).toBeGreaterThan(0);
    });

    it('does not open a BroadcastChannel in standalone mode', () => {
      (globalThis as any).navigator = {};
      (globalThis.BroadcastChannel as any).mockClear?.();
      coordinator = new SSECoordinator();

      coordinator.connect({ url: TEST_URL, eventTypes: TEST_EVENTS, onEvent: () => {} });

      expect((globalThis.BroadcastChannel as any).mock.calls.length).toBe(0);
    });

    it('disconnect() in standalone closes the EventSource without throwing', () => {
      (globalThis as any).navigator = {};
      coordinator = new SSECoordinator();
      coordinator.connect({ url: TEST_URL, eventTypes: TEST_EVENTS, onEvent: () => {} });

      const closeSpy = mock(() => {});
      (coordinator as any).closeEventSource = closeSpy;

      expect(() => coordinator.disconnect()).not.toThrow();
      expect(closeSpy).toHaveBeenCalled();
    });
  });
});
