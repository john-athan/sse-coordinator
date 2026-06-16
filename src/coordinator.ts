import type { SSECoordinatorOptions, SSEEvent } from './types';

const DEFAULT_CHANNEL_NAME = 'sse-coordinator';

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

    if (this.channel) {
      this.cleanup();
    }

    this.currentOptions = options;
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

  private async runAsLeader(): Promise<void> {
    this.isLeaderTab = true;
    this.reconnectAttempts = 0;
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

    const { url, eventTypes, withCredentials = false } = this.currentOptions;
    this.eventSource = new EventSource(url, { withCredentials });

    this.eventSource.onopen = () => {
      this.reconnectAttempts = 0;
      this.currentOptions?.onConnectionChange?.(true);
      this.broadcast({ type: 'connection-state', connected: true });
      this.log('debug', 'Leader connection established');
    };

    eventTypes.forEach(type => {
      this.eventSource!.addEventListener(type, (e: MessageEvent) => {
        try {
          const event: SSEEvent = {
            type,
            data: JSON.parse(e.data),
            id: e.lastEventId,
            timestamp: new Date().toISOString(),
          };
          this.currentOptions?.onEvent(event);
          this.broadcastEvent(event);
        } catch {
          this.log('error', `Failed to parse event: ${type}`);
        }
      });
    });

    this.eventSource.onerror = () => {
      this.currentOptions?.onConnectionChange?.(false);
      this.broadcast({ type: 'connection-state', connected: false });
      this.log('warn', 'Leader connection error');
      this.handleReconnect();
    };
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
    if (this.channel) {
      this.channel.close();
      this.channel = null;
    }
    this.isLeaderTab = false;
    this.releaseLock = null;
    this.lockAbortController = null;
    this.currentOptions = null;
    this.reconnectAttempts = 0;
  }

  private handleReconnect(): void {
    if (!this.isLeaderTab || !this.currentOptions) return;

    const maxAttempts = this.currentOptions.maxReconnectAttempts ?? 10;
    if (this.reconnectAttempts >= maxAttempts) {
      this.log('warn', 'Max reconnection attempts reached');
      this.currentOptions.onError?.(new Error('Max reconnection attempts reached'));
      return;
    }

    this.reconnectAttempts++;
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

  private broadcast(message: Omit<BroadcastMessage, 'tabId'>): void {
    this.channel?.postMessage({ ...message, tabId: this.tabId });
  }

  private log(level: 'debug' | 'info' | 'warn' | 'error', message: string): void {
    this.currentOptions?.logger?.[level](`[SSECoordinator] ${message}`);
  }
}
