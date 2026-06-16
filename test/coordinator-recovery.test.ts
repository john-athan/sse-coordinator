import { describe, it, expect, beforeEach, afterEach, mock, jest } from 'bun:test';
import { SSECoordinator } from '../src/coordinator';

const TEST_URL = 'https://api.example.com/events/stream';
const TEST_EVENTS = ['message'];

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
  removeEventListener() {}
  close() { this.readyState = 2; }
  dispatchEvent() { return true; }
  fireOpen() { this.readyState = 1; this.onopen?.(new Event('open')); }
  fireError() { this.readyState = 0; this.onerror?.(new Event('error')); }
}

function createLocksMock() {
  const held = new Map<string, boolean>();
  const queues = new Map<string, Array<() => void>>();
  const runNext = (name: string) => {
    const q = queues.get(name) ?? [];
    const next = q.shift();
    if (next) next();
  };
  const request = (name: string, optionsOrCallback: any, maybeCallback?: any): Promise<void> => {
    const hasOptions = typeof optionsOrCallback === 'object' && optionsOrCallback !== null;
    const options = hasOptions ? optionsOrCallback : {};
    const callback = hasOptions ? maybeCallback : optionsOrCallback;
    const signal: AbortSignal | undefined = options.signal;
    return new Promise<void>((resolve, reject) => {
      if (signal?.aborted) { reject(new DOMException('Aborted', 'AbortError')); return; }
      const run = async () => {
        held.set(name, true);
        try { await callback({}); resolve(); }
        catch (e) { reject(e); }
        finally { held.set(name, false); runNext(name); }
      };
      if (signal) {
        signal.addEventListener('abort', () => {
          const q = queues.get(name);
          if (q) { const idx = q.indexOf(run); if (idx >= 0) { q.splice(idx, 1); reject(new DOMException('Aborted', 'AbortError')); } }
        }, { once: true });
      }
      if (!held.get(name)) { run(); }
      else { if (!queues.has(name)) queues.set(name, []); queues.get(name)!.push(run); }
    });
  };
  return { request };
}

interface ListenerStore {
  handlers: Map<string, Array<() => void>>;
  addEventListener: (type: string, h: () => void) => void;
  removeEventListener: (type: string, h: () => void) => void;
  fire: (type: string) => void;
}

function createListenerStore(): ListenerStore {
  const handlers = new Map<string, Array<() => void>>();
  return {
    handlers,
    addEventListener(type, h) {
      if (!handlers.has(type)) handlers.set(type, []);
      handlers.get(type)!.push(h);
    },
    removeEventListener(type, h) {
      const arr = handlers.get(type);
      if (arr) { const i = arr.indexOf(h); if (i >= 0) arr.splice(i, 1); }
    },
    fire(type) { (handlers.get(type) ?? []).forEach(h => h()); },
  };
}

let createdEventSources: FunctionalEventSourceMock[] = [];
let coordinator: SSECoordinator;
let documentStore: ListenerStore;
let windowStore: ListenerStore;

beforeEach(() => {
  jest.useFakeTimers();
  createdEventSources = [];

  globalThis.EventSource = class extends FunctionalEventSourceMock {
    constructor(url: string, opts?: any) {
      super(url, opts);
      createdEventSources.push(this);
    }
  } as any;

  globalThis.BroadcastChannel = mock(() => ({
    postMessage: mock(() => {}),
    close: mock(() => {}),
    addEventListener: mock(() => {}),
    removeEventListener: mock(() => {}),
  })) as any;

  documentStore = createListenerStore();
  windowStore = createListenerStore();
  (globalThis as any).document = {
    visibilityState: 'visible',
    addEventListener: documentStore.addEventListener,
    removeEventListener: documentStore.removeEventListener,
  };
  (globalThis as any).window = {
    addEventListener: windowStore.addEventListener,
    removeEventListener: windowStore.removeEventListener,
    location: { href: TEST_URL },
  };
  (globalThis as any).navigator = { locks: createLocksMock() };
});

afterEach(() => {
  coordinator?.disconnect();
  jest.useRealTimers();
  delete (globalThis as any).document;
  delete (globalThis as any).window;
});

describe('SSECoordinator - focus/online recovery', () => {
  it('reconnects on visibilitychange when the connection is down', () => {
    coordinator = new SSECoordinator();
    coordinator.connect({ url: TEST_URL, eventTypes: TEST_EVENTS, onEvent: () => {} });

    createdEventSources[0].fireError();
    createdEventSources[0].close();
    const before = createdEventSources.length;

    documentStore.fire('visibilitychange');

    expect(createdEventSources.length).toBe(before + 1);
  });

  it('does not reconnect on visibilitychange when the connection is open', () => {
    coordinator = new SSECoordinator();
    coordinator.connect({ url: TEST_URL, eventTypes: TEST_EVENTS, onEvent: () => {} });

    createdEventSources[0].fireOpen();
    const before = createdEventSources.length;

    documentStore.fire('visibilitychange');

    expect(createdEventSources.length).toBe(before);
  });

  it('does not reconnect on visibilitychange when the tab is hidden', () => {
    coordinator = new SSECoordinator();
    coordinator.connect({ url: TEST_URL, eventTypes: TEST_EVENTS, onEvent: () => {} });

    createdEventSources[0].fireError();
    createdEventSources[0].close();
    const before = createdEventSources.length;

    (globalThis as any).document.visibilityState = 'hidden';
    documentStore.fire('visibilitychange');

    expect(createdEventSources.length).toBe(before);
  });

  it('reconnects on the online event when the connection is down', () => {
    coordinator = new SSECoordinator();
    coordinator.connect({ url: TEST_URL, eventTypes: TEST_EVENTS, onEvent: () => {} });

    createdEventSources[0].fireError();
    createdEventSources[0].close();
    const before = createdEventSources.length;

    windowStore.fire('online');

    expect(createdEventSources.length).toBe(before + 1);
  });

  it('resets reconnect attempts when recovering', () => {
    coordinator = new SSECoordinator();
    coordinator.connect({ url: TEST_URL, eventTypes: TEST_EVENTS, onEvent: () => {} });

    (coordinator as any).reconnectAttempts = 9;
    createdEventSources[0].close();

    documentStore.fire('visibilitychange');

    expect((coordinator as any).reconnectAttempts).toBe(0);
  });

  it('removes the recovery listeners on disconnect', () => {
    coordinator = new SSECoordinator();
    coordinator.connect({ url: TEST_URL, eventTypes: TEST_EVENTS, onEvent: () => {} });

    coordinator.disconnect();

    expect(documentStore.handlers.get('visibilitychange') ?? []).toHaveLength(0);
    expect(windowStore.handlers.get('online') ?? []).toHaveLength(0);
  });
});

describe('SSECoordinator - reconnectForever', () => {
  it('keeps reconnecting at the capped interval after max attempts', () => {
    const onError = mock(() => {});
    coordinator = new SSECoordinator();
    coordinator.connect({
      url: TEST_URL,
      eventTypes: TEST_EVENTS,
      reconnectForever: true,
      onEvent: () => {},
      onError,
    });

    (coordinator as any).reconnectAttempts = 10;
    const before = createdEventSources.length;
    (coordinator as any).handleReconnect();

    jest.advanceTimersByTime(30000);

    expect(createdEventSources.length).toBe(before + 1);
    expect(onError).toHaveBeenCalledTimes(1);
  });

  it('only calls onError once even across repeated failures', () => {
    const onError = mock(() => {});
    coordinator = new SSECoordinator();
    coordinator.connect({
      url: TEST_URL,
      eventTypes: TEST_EVENTS,
      reconnectForever: true,
      onEvent: () => {},
      onError,
    });

    (coordinator as any).reconnectAttempts = 10;
    (coordinator as any).handleReconnect();
    (coordinator as any).handleReconnect();

    expect(onError).toHaveBeenCalledTimes(1);
  });

  it('still gives up after max attempts when reconnectForever is false', () => {
    const onError = mock(() => {});
    coordinator = new SSECoordinator();
    coordinator.connect({ url: TEST_URL, eventTypes: TEST_EVENTS, onEvent: () => {}, onError });

    (coordinator as any).reconnectAttempts = 10;
    const before = createdEventSources.length;
    (coordinator as any).handleReconnect();
    jest.advanceTimersByTime(30000);

    expect(createdEventSources.length).toBe(before);
    expect(onError).toHaveBeenCalledTimes(1);
  });
});
