export interface SSEEvent {
  type: string;
  data: unknown;
  id: string;
  timestamp: string;
}

export interface Logger {
  debug: (message: string, ...args: unknown[]) => void;
  info: (message: string, ...args: unknown[]) => void;
  warn: (message: string, ...args: unknown[]) => void;
  error: (message: string, ...args: unknown[]) => void;
}

export interface SSECoordinatorOptions {
  url: string;
  eventTypes: string[];
  channelName?: string;
  withCredentials?: boolean;
  /**
   * Whether to `JSON.parse` each event's `data` (default `true`). When `true`,
   * payloads that fail to parse are logged and skipped. Set to `false` for
   * plain-text or custom-format streams: `data` is then delivered as the raw
   * string, untouched.
   */
  parseJson?: boolean;
  maxReconnectAttempts?: number;
  /**
   * When set, the most recent event id is appended to the connection URL as
   * this query parameter on every (re)connect and on leader handover, letting
   * a cooperating server resume the stream from where it left off. The server
   * must read this parameter. Omit it to disable resumption.
   */
  lastEventIdParam?: string;
  logger?: Logger;
  onEvent: (event: SSEEvent) => void;
  onError?: (error: Error) => void;
  onConnectionChange?: (connected: boolean) => void;
}
