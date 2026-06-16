import type { SSECoordinatorOptions, SSEEvent } from './types';

const DEFAULT_CHANNEL_NAME = 'sse-coordinator';

function hasWebLocks(): boolean {
  return (
    typeof navigator !== 'undefined' &&
    navigator.locks != null &&
    typeof navigator.locks.request === 'function'
  );
}

interface BroadcastMessage {
  type: 'sse-event' | 'connection-state';
  tabId: string;
  event?: SSEEvent;
  connected?: boolean;
}

export class SSECoordinator {
  private channel: BroadcastChannel | null = null;
  private eventSource: EventSource | null = null;
  private isLeaderTab = false;
  private tabId: string;
  private currentOptions: SSECoordinatorOptions | null = null;
  private releaseLock: (() => void) | null = null;
  private lockAbortController: AbortController | null = null;
  private reconnectAttempts = 0;
  private reconnectTimeoutId: number | null = null;
  private reconnectGaveUp = false;
  private lastEventId: string | null = null;
  private onVisibilityChange: (() => void) | null = null;
  private onOnline: (() => void) | null = null;

  constructor() {
    this.tabId = `tab-${
      typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function'
        ? crypto.randomUUID()
        : `${Math.random().toString(36).slice(2)}${Math.random().toString(36).slice(2)}`
    }`;
  }

  connect(options: SSECoordinatorOptions): void {
    let parsedUrl: URL;
    try {
      const base = typeof window !== 'undefined' ? window.location.href : undefined;
      parsedUrl = new URL(options.url, base);
    } catch {
      throw new Error(`Invalid URL: ${options.url}`);
    }
    if (parsedUrl.protocol !== 'https:' && parsedUrl.protocol !== 'http:') {
      throw new Error(`URL must use http or https protocol: ${options.url}`);
    }

    if (this.channel || this.isLeaderTab) {
      this.cleanup();
    }

    this.currentOptions = options;

    // Recover the connection when the tab regains focus or the network comes
    // back. Background tabs are frozen and machines sleep, both of which leave
    // the EventSource dead with the backoff timer throttled or already given
    // up. These events are the reliable signal that it's worth trying again.
    this.registerRecoveryListeners();

    // The Web Locks API is what guarantees a single leader across tabs. Without
    // it there is no safe way to coordinate, so degrade to standalone: this tab
    // runs its own EventSource. Every tab does the same — no shared connection,
    // but the app keeps working. (Web Locks is in all evergreen browsers;
    // notable gaps are Firefox < 96 and any pre-15.4 Safari.)
    if (!hasWebLocks()) {
      this.log(
        'warn',
        'Web Locks API unavailable; running standalone (one connection per tab, no cross-tab coordination)'
      );
      this.runStandalone();
      return;
    }

    const channelName = options.channelName ?? DEFAULT_CHANNEL_NAME;
    const lockName = `${channelName}-leader`;

    this.channel = new BroadcastChannel(channelName);
    this.channel.addEventListener('message', this.handleBroadcastMessage.bind(this));

    this.lockAbortController = new AbortController();

    // Browser ensures exactly one tab holds the lock at a time.
    // Other tabs queue automatically and take over when the leader releases.
    navigator.locks.request(
      lockName,
      { signal: this.lockAbortController.signal },
      async () => {
        await this.runAsLeader();
      }
    ).catch((e: Error) => {
      if (e.name !== 'AbortError') {
        this.log('error', `Lock request failed: ${e.message}`);
      }
    });
  }

  disconnect(): void {
    this.lockAbortController?.abort(); // cancel pending lock request if still queued
    this.releaseLock?.();              // release held lock if leader
    this.cleanup();
  }

  isLeader(): boolean {
    return this.isLeaderTab;
  }

  // Kept as public API for edge cases and testing.
  // Releases leadership so the next queued tab can take over.
  demoteToFollower(): void {
    if (!this.isLeaderTab) return;
    this.log('info', 'Demoting to follower');
    this.isLeaderTab = false;
    this.closeEventSource();
    this.releaseLock?.();
    this.releaseLock = null;
  }

  broadcastEvent(event: SSEEvent): void {
    this.broadcast({ type: 'sse-event', event });
  }

  handleBroadcastMessage(messageOrEvent: BroadcastMessage | { data: unknown }): void {
    const raw: unknown =
      messageOrEvent && typeof messageOrEvent === 'object' && 'data' in messageOrEvent
        ? (messageOrEvent as { data: unknown }).data
        : messageOrEvent;

    if (!this.isValidBroadcastMessage(raw)) return;
    const message = raw;

    if (message.tabId === this.tabId) return;

    switch (message.type) {
      case 'sse-event':
        if (message.event && this.currentOptions) {
          // Track the id even as a follower, so a resume URL is correct if this
          // tab is later promoted to leader.
          if (message.event.id) this.lastEventId = message.event.id;
          this.currentOptions.onEvent(message.event);
        }
        break;

      case 'connection-state':
        if (!this.isLeaderTab && message.connected !== undefined) {
          this.currentOptions?.onConnectionChange?.(message.connected);
        }
        break;
    }
  }

  // Degraded path when the Web Locks API is unavailable: become an
  // uncoordinated leader with a private EventSource. There is no channel and
  // no failover — each tab is on its own.
  private runStandalone(): void {
    this.isLeaderTab = true;
    this.reconnectAttempts = 0;
    this.reconnectGaveUp = false;
    this.createEventSource();
  }

  private async runAsLeader(): Promise<void> {
    this.isLeaderTab = true;
    this.reconnectAttempts = 0;
    this.reconnectGaveUp = false;
    this.log('info', 'Promoting to leader');
    this.createEventSource();
    return new Promise<void>(resolve => {
      this.releaseLock = resolve;
    });
  }

  private isValidBroadcastMessage(msg: unknown): msg is BroadcastMessage {
    if (!msg || typeof msg !== 'object') return false;
    const m = msg as Record<string, unknown>;
    return m.type === 'sse-event' || m.type === 'connection-state';
  }

  private createEventSource(): void {
    if (this.eventSource || !this.currentOptions) return;

    const { eventTypes, withCredentials = false, parseJson = true } = this.currentOptions;
    this.eventSource = new EventSource(this.resumeUrl(), { withCredentials });

    this.eventSource.onopen = () => {
      this.reconnectAttempts = 0;
      this.reconnectGaveUp = false;
      this.currentOptions?.onConnectionChange?.(true);
      this.broadcast({ type: 'connection-state', connected: true });
      this.log('debug', 'Leader connection established');
    };

    eventTypes.forEach(type => {
      this.eventSource!.addEventListener(type, (e: MessageEvent) => {
        let data: unknown;
        if (parseJson) {
          try {
            data = JSON.parse(e.data);
          } catch {
            // Malformed payload: log and skip rather than deliver garbage.
            // Use parseJson: false for non-JSON streams.
            this.log('error', `Failed to parse event as JSON: ${type}`);
            return;
          }
        } else {
          data = e.data;
        }

        if (e.lastEventId) this.lastEventId = e.lastEventId;
        const event: SSEEvent = {
          type,
          data,
          id: e.lastEventId,
          timestamp: new Date().toISOString(),
        };
        this.currentOptions?.onEvent(event);
        this.broadcastEvent(event);
      });
    });

    this.eventSource.onerror = () => {
      this.currentOptions?.onConnectionChange?.(false);
      this.broadcast({ type: 'connection-state', connected: false });
      this.log('warn', 'Leader connection error');
      this.handleReconnect();
    };
  }

  // Builds the URL used to open the EventSource. When lastEventIdParam is
  // configured and an event id is known, appends it so a cooperating server can
  // resume the stream — including after a manual reconnect or leader handover.
  private resumeUrl(): string {
    const opts = this.currentOptions!;
    if (!opts.lastEventIdParam || !this.lastEventId) return opts.url;
    try {
      const base = typeof window !== 'undefined' ? window.location.href : undefined;
      const u = new URL(opts.url, base);
      u.searchParams.set(opts.lastEventIdParam, this.lastEventId);
      return u.toString();
    } catch {
      return opts.url;
    }
  }

  private closeEventSource(): void {
    if (this.eventSource) {
      this.eventSource.close();
      this.eventSource = null;
      this.currentOptions?.onConnectionChange?.(false);
    }
  }

  private cleanup(): void {
    this.closeEventSource();
    this.stopReconnectTimer();
    this.removeRecoveryListeners();
    if (this.channel) {
      this.channel.close();
      this.channel = null;
    }
    this.isLeaderTab = false;
    this.releaseLock = null;
    this.lockAbortController = null;
    this.currentOptions = null;
    this.reconnectAttempts = 0;
    this.reconnectGaveUp = false;
    this.lastEventId = null;
  }

  private handleReconnect(): void {
    if (!this.isLeaderTab || !this.currentOptions) return;

    const maxAttempts = this.currentOptions.maxReconnectAttempts ?? 10;
    const reconnectForever = this.currentOptions.reconnectForever ?? false;

    if (this.reconnectAttempts >= maxAttempts) {
      if (!this.reconnectGaveUp) {
        this.reconnectGaveUp = true;
        this.log(
          'warn',
          reconnectForever
            ? 'Max reconnection attempts reached; continuing to retry at capped interval'
            : 'Max reconnection attempts reached'
        );
        this.currentOptions.onError?.(new Error('Max reconnection attempts reached'));
      }
      // When not retrying forever, stop here. Recovery can still be triggered by
      // a focus/online event via recoverConnection().
      if (!reconnectForever) return;
    } else {
      this.reconnectAttempts++;
    }

    const delay = Math.min(1000 * Math.pow(2, this.reconnectAttempts), 30000);
    this.log('debug', `Reconnecting in ${delay}ms (attempt ${this.reconnectAttempts}/${maxAttempts})`);

    this.stopReconnectTimer();
    this.reconnectTimeoutId = setTimeout(() => {
      this.reconnectTimeoutId = null;
      if (this.isLeaderTab) {
        this.closeEventSource();
        this.createEventSource();
      }
    }, delay) as unknown as number;
  }

  private stopReconnectTimer(): void {
    if (this.reconnectTimeoutId) {
      clearTimeout(this.reconnectTimeoutId);
      this.reconnectTimeoutId = null;
    }
  }

  private registerRecoveryListeners(): void {
    if (typeof document !== 'undefined' && typeof document.addEventListener === 'function') {
      if (!this.onVisibilityChange) {
        this.onVisibilityChange = () => {
          if (document.visibilityState === 'visible') this.recoverConnection();
        };
        document.addEventListener('visibilitychange', this.onVisibilityChange);
      }
    }
    if (typeof window !== 'undefined' && typeof window.addEventListener === 'function') {
      if (!this.onOnline) {
        this.onOnline = () => this.recoverConnection();
        window.addEventListener('online', this.onOnline);
      }
    }
  }

  private removeRecoveryListeners(): void {
    if (this.onVisibilityChange && typeof document !== 'undefined') {
      document.removeEventListener('visibilitychange', this.onVisibilityChange);
    }
    this.onVisibilityChange = null;
    if (this.onOnline && typeof window !== 'undefined') {
      window.removeEventListener('online', this.onOnline);
    }
    this.onOnline = null;
  }

  // Force an immediate reconnect if we are the leader and the connection is not
  // currently open. Triggered by tab focus or network-online — the moments when
  // a silently-dead connection should be revived without waiting on (or despite
  // having exhausted) the exponential-backoff timer.
  private recoverConnection(): void {
    if (!this.isLeaderTab || !this.currentOptions) return;
    const isOpen = this.eventSource != null && this.eventSource.readyState === 1;
    if (isOpen) return;
    this.log('info', 'Recovering connection after focus/online');
    this.reconnectAttempts = 0;
    this.reconnectGaveUp = false;
    this.stopReconnectTimer();
    this.closeEventSource();
    this.createEventSource();
  }

  private broadcast(message: Omit<BroadcastMessage, 'tabId'>): void {
    this.channel?.postMessage({ ...message, tabId: this.tabId });
  }

  private log(level: 'debug' | 'info' | 'warn' | 'error', message: string): void {
    this.currentOptions?.logger?.[level](`[SSECoordinator] ${message}`);
  }
}
