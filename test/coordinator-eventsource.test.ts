import { describe, it, expect, beforeEach, afterEach, mock, jest } from 'bun:test';
import { SSECoordinator } from '../src/coordinator';
import type { SSEEvent } from '../src/types';

const TEST_URL = 'https://api.example.com/events/stream';
const TEST_EVENTS = ['processing.request.started', 'notification.created', 'message'];

class FunctionalEventSourceMock {
  url: string;
  withCredentials: boolean;
  onopen: ((event: Event) => void) | null = null;
  onerror: ((event: Event) => void) | null = null;
  readyState = 0;

  private listeners: Map<string, Array<(e: MessageEvent) => void>> = new Map();

  constructor(url: string, opts?: { withCredentials?: boolean }) {
    this.url = url;
    this.withCredentials = opts?.withCredentials ?? false;
  }

  addEventListener(type: string, handler: (e: MessageEvent) => void) {
    if (!this.listeners.has(type)) this.listeners.set(type, []);
    this.listeners.get(type)!.push(handler);
  }

  removeEventListener(type: string, handler: (e: MessageEvent) => void) {
    const arr = this.listeners.get(type);
    if (arr) {
      const idx = arr.indexOf(handler);
      if (idx !== -1) arr.splice(idx, 1);
    }
  }

  close() { this.readyState = 2; }
  dispatchEvent() { return true; }

  fireOpen() {
    this.readyState = 1;
    this.onopen?.(new Event('open'));
  }

  fireError() {
    this.readyState = 0;
    this.onerror?.(new Event('error'));
  }

  fireNamedEvent(type: string, data: unknown, lastEventId = '') {
    const handlers = this.listeners.get(type) ?? [];
    const e = new MessageEvent(type, { data: JSON.stringify(data), lastEventId });
    handlers.forEach(h => h(e));
  }
}

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
          if (q) { const idx = q.indexOf(run); if (idx >= 0) { q.splice(idx, 1); reject(new DOMException('Aborted', 'AbortError')); } }
        }, { once: true });
      }

      if (!held.get(_name)) { run(); }
      else { if (!queues.has(_name)) queues.set(_name, []); queues.get(_name)!.push(run); }
    });
  };

  return { request };
}

let createdEventSources: FunctionalEventSourceMock[] = [];
let coordinator: SSECoordinator;
let broadcastChannelMock: any;

beforeEach(() => {
  jest.useFakeTimers();
  createdEventSources = [];

  globalThis.EventSource = class extends FunctionalEventSourceMock {
    constructor(url: string, opts?: any) {
      super(url, opts);
      createdEventSources.push(this);
    }
  } as any;

  broadcastChannelMock = {
    postMessage: mock(() => {}),
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

describe('SSECoordinator - EventSource creation', () => {
  it('creates EventSource without credentials by default when promoted to leader', () => {
    coordinator = new SSECoordinator();
    coordinator.connect({ url: TEST_URL, eventTypes: TEST_EVENTS, onEvent: () => {} });

    expect(coordinator.isLeader()).toBe(true);
    expect(createdEventSources).toHaveLength(1);
    expect(createdEventSources[0].withCredentials).toBe(false);
    expect(createdEventSources[0].url).toBe(TEST_URL);
  });

  it('creates EventSource without credentials when withCredentials is false', () => {
    coordinator = new SSECoordinator();
    coordinator.connect({
      url: TEST_URL,
      eventTypes: TEST_EVENTS,
      withCredentials: false,
      onEvent: () => {},
    });

    expect(createdEventSources[0].withCredentials).toBe(false);
  });

  it('calls onConnectionChange(true) when EventSource opens', () => {
    const onConnectionChange = mock(() => {});
    coordinator = new SSECoordinator();
    coordinator.connect({ url: TEST_URL, eventTypes: TEST_EVENTS, onEvent: () => {}, onConnectionChange });

    createdEventSources[0].fireOpen();

    expect(onConnectionChange).toHaveBeenCalledWith(true);
  });

  it('resets reconnectAttempts to 0 on successful open', () => {
    coordinator = new SSECoordinator();
    coordinator.connect({ url: TEST_URL, eventTypes: TEST_EVENTS, onEvent: () => {} });

    (coordinator as any).reconnectAttempts = 5;
    createdEventSources[0].fireOpen();

    expect((coordinator as any).reconnectAttempts).toBe(0);
  });

  it('calls onConnectionChange(false) when EventSource errors', () => {
    const onConnectionChange = mock(() => {});
    coordinator = new SSECoordinator();
    coordinator.connect({ url: TEST_URL, eventTypes: TEST_EVENTS, onEvent: () => {}, onConnectionChange });

    createdEventSources[0].fireOpen();
    onConnectionChange.mockClear();

    createdEventSources[0].fireError();

    expect(onConnectionChange).toHaveBeenCalledWith(false);
  });

  it('broadcasts connection-state to followers when EventSource opens', () => {
    coordinator = new SSECoordinator();
    coordinator.connect({ url: TEST_URL, eventTypes: TEST_EVENTS, onEvent: () => {} });

    const before = (broadcastChannelMock.postMessage as any).mock.calls.length;
    createdEventSources[0].fireOpen();

    const newMessages = (broadcastChannelMock.postMessage as any).mock.calls.slice(before);
    const stateMessages = newMessages.filter((c: any[]) => c[0]?.type === 'connection-state');
    expect(stateMessages).toHaveLength(1);
    expect(stateMessages[0][0].connected).toBe(true);
  });
});

describe('SSECoordinator - event handling', () => {
  it('calls onEvent when a named SSE event fires', () => {
    const receivedEvents: SSEEvent[] = [];
    coordinator = new SSECoordinator();
    coordinator.connect({ url: TEST_URL, eventTypes: TEST_EVENTS, onEvent: e => receivedEvents.push(e) });

    createdEventSources[0].fireNamedEvent('processing.request.started', { id: 'req-1' }, 'ev-42');

    expect(receivedEvents).toHaveLength(1);
    expect(receivedEvents[0].type).toBe('processing.request.started');
    expect(receivedEvents[0].data).toEqual({ id: 'req-1' });
    expect(receivedEvents[0].id).toBe('ev-42');
    expect(receivedEvents[0].timestamp).toBeDefined();
  });

  it('broadcasts events to other tabs via BroadcastChannel', () => {
    coordinator = new SSECoordinator();
    coordinator.connect({ url: TEST_URL, eventTypes: TEST_EVENTS, onEvent: () => {} });

    const before = (broadcastChannelMock.postMessage as any).mock.calls.length;
    createdEventSources[0].fireNamedEvent('notification.created', { id: 99 });

    const newMessages = (broadcastChannelMock.postMessage as any).mock.calls.slice(before);
    const sseMessages = newMessages.filter((call: any[]) => call[0]?.type === 'sse-event');
    expect(sseMessages).toHaveLength(1);
    expect(sseMessages[0][0].event.type).toBe('notification.created');
  });

  it('does not throw when event data is invalid JSON', () => {
    coordinator = new SSECoordinator();
    coordinator.connect({ url: TEST_URL, eventTypes: TEST_EVENTS, onEvent: () => {} });

    const handlers = (createdEventSources[0] as any).listeners.get('processing.request.started') ?? [];
    const badEvent = new MessageEvent('processing.request.started', { data: 'not-json', lastEventId: '' });

    expect(() => handlers.forEach((h: any) => h(badEvent))).not.toThrow();
  });

  it('only listens to specified eventTypes', () => {
    const receivedEvents: SSEEvent[] = [];
    coordinator = new SSECoordinator();
    coordinator.connect({
      url: TEST_URL,
      eventTypes: ['notification.created'],
      onEvent: e => receivedEvents.push(e),
    });

    createdEventSources[0].fireNamedEvent('notification.created', { id: 1 });

    const unregisteredHandlers =
      (createdEventSources[0] as any).listeners.get('processing.request.started') ?? [];
    expect(unregisteredHandlers).toHaveLength(0);
    expect(receivedEvents).toHaveLength(1);
  });
});

describe('SSECoordinator - reconnect logic', () => {
  it('does not reconnect when not leader', () => {
    coordinator = new SSECoordinator();
    coordinator.connect({ url: TEST_URL, eventTypes: TEST_EVENTS, onEvent: () => {} });

    (coordinator as any).isLeaderTab = false;
    (coordinator as any).handleReconnect();

    expect(createdEventSources).toHaveLength(1); // only the initial one
  });

  it('calls onError when max reconnect attempts reached', () => {
    const onError = mock(() => {});
    coordinator = new SSECoordinator();
    coordinator.connect({ url: TEST_URL, eventTypes: TEST_EVENTS, onEvent: () => {}, onError });

    (coordinator as any).reconnectAttempts = 10;
    (coordinator as any).handleReconnect();

    expect(onError).toHaveBeenCalledWith(expect.any(Error));
    expect((onError as any).mock.calls[0][0].message).toContain('Max reconnection');
  });

  it('respects custom maxReconnectAttempts', () => {
    const onError = mock(() => {});
    coordinator = new SSECoordinator();
    coordinator.connect({
      url: TEST_URL,
      eventTypes: TEST_EVENTS,
      maxReconnectAttempts: 3,
      onEvent: () => {},
      onError,
    });

    (coordinator as any).reconnectAttempts = 3;
    (coordinator as any).handleReconnect();

    expect(onError).toHaveBeenCalled();
  });

  it('increments reconnectAttempts on each reconnect', () => {
    coordinator = new SSECoordinator();
    coordinator.connect({ url: TEST_URL, eventTypes: TEST_EVENTS, onEvent: () => {} });

    const before = (coordinator as any).reconnectAttempts;
    (coordinator as any).handleReconnect();

    expect((coordinator as any).reconnectAttempts).toBe(before + 1);
  });

  it('schedules reconnect with exponential backoff', () => {
    coordinator = new SSECoordinator();
    coordinator.connect({ url: TEST_URL, eventTypes: TEST_EVENTS, onEvent: () => {} });

    const initialSources = createdEventSources.length;
    (coordinator as any).reconnectAttempts = 1;
    (coordinator as any).handleReconnect();

    jest.advanceTimersByTime(5000);
    expect(createdEventSources.length).toBeGreaterThan(initialSources);
  });

  it('caps reconnect delay at 30 seconds', () => {
    coordinator = new SSECoordinator();
    coordinator.connect({ url: TEST_URL, eventTypes: TEST_EVENTS, onEvent: () => {} });

    const initialSources = createdEventSources.length;
    (coordinator as any).reconnectAttempts = 9;
    (coordinator as any).handleReconnect();

    jest.advanceTimersByTime(30001);
    expect(createdEventSources.length).toBeGreaterThan(initialSources);
  });

  it('does not reconnect if no longer leader when timer fires', () => {
    coordinator = new SSECoordinator();
    coordinator.connect({ url: TEST_URL, eventTypes: TEST_EVENTS, onEvent: () => {} });

    const initialSources = createdEventSources.length;
    (coordinator as any).handleReconnect();

    coordinator.demoteToFollower();
    jest.advanceTimersByTime(5000);

    expect(createdEventSources.length).toBe(initialSources);
  });

  it('cancels pending reconnect timer when disconnect() is called', () => {
    coordinator = new SSECoordinator();
    coordinator.connect({ url: TEST_URL, eventTypes: TEST_EVENTS, onEvent: () => {} });

    const initialSources = createdEventSources.length;
    (coordinator as any).handleReconnect();

    expect((coordinator as any).reconnectTimeoutId).not.toBeNull();

    coordinator.disconnect();

    expect((coordinator as any).reconnectTimeoutId).toBeNull();

    jest.advanceTimersByTime(5000);
    expect(createdEventSources.length).toBe(initialSources);
  });
});

describe('SSECoordinator - closeEventSource', () => {
  it('calls onConnectionChange(false) when eventSource is closed via demoteToFollower', () => {
    const onConnectionChange = mock(() => {});
    coordinator = new SSECoordinator();
    coordinator.connect({ url: TEST_URL, eventTypes: TEST_EVENTS, onEvent: () => {}, onConnectionChange });

    createdEventSources[0].fireOpen();
    onConnectionChange.mockClear();

    coordinator.demoteToFollower();

    expect(onConnectionChange).toHaveBeenCalledWith(false);
  });

  it('sets eventSource to null after closing', () => {
    coordinator = new SSECoordinator();
    coordinator.connect({ url: TEST_URL, eventTypes: TEST_EVENTS, onEvent: () => {} });

    coordinator.demoteToFollower();

    expect((coordinator as any).eventSource).toBeNull();
  });
});
