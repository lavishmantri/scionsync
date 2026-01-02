export interface WebSocketMessage {
  type: 'yjs-update' | 'structure-update' | 'binary-update' | 'ping' | 'pong' | 'sync-request' | 'sync-response';
  vaultName: string;
  deviceId: string;
  fileId?: string;
  payload?: string; // base64 encoded
  timestamp: number;
}

export type ConnectionState = 'disconnected' | 'connecting' | 'connected' | 'reconnecting';

export type MessageHandler = (message: WebSocketMessage) => void;
export type ConnectionStateHandler = (state: ConnectionState) => void;

export class WebSocketClient {
  private socket: WebSocket | null = null;
  private serverUrl: string;
  private vaultName: string;
  private deviceId: string;
  private messageHandlers: Map<string, MessageHandler[]> = new Map();
  private connectionStateHandlers: ConnectionStateHandler[] = [];
  private reconnectAttempts = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private pingTimer: ReturnType<typeof setInterval> | null = null;
  private connectionState: ConnectionState = 'disconnected';
  private manualDisconnect = false;

  private static readonly MAX_RECONNECT_ATTEMPTS = 10;
  private static readonly BASE_RECONNECT_DELAY = 1000; // 1 second
  private static readonly MAX_RECONNECT_DELAY = 30000; // 30 seconds
  private static readonly PING_INTERVAL = 25000; // 25 seconds

  constructor(serverUrl: string, vaultName: string, deviceId: string) {
    this.serverUrl = serverUrl;
    this.vaultName = vaultName;
    this.deviceId = deviceId;
  }

  /**
   * Connect to the WebSocket server
   */
  async connect(): Promise<void> {
    if (this.socket && (this.socket.readyState === WebSocket.CONNECTING || this.socket.readyState === WebSocket.OPEN)) {
      console.log('[WS Client] Already connected or connecting');
      return;
    }

    this.manualDisconnect = false;
    this.setConnectionState('connecting');

    return new Promise((resolve, reject) => {
      try {
        // Convert HTTP URL to WebSocket URL
        const wsUrl = this.getWebSocketUrl();
        console.log(`[WS Client] Connecting to ${wsUrl}`);

        this.socket = new WebSocket(wsUrl);

        const timeout = setTimeout(() => {
          if (this.socket?.readyState === WebSocket.CONNECTING) {
            this.socket.close();
            reject(new Error('Connection timeout'));
          }
        }, 10000);

        this.socket.onopen = () => {
          clearTimeout(timeout);
          console.log('[WS Client] Connected');
          this.reconnectAttempts = 0;
          this.setConnectionState('connected');
          this.startPingInterval();
          resolve();
        };

        this.socket.onclose = (event) => {
          clearTimeout(timeout);
          console.log(`[WS Client] Disconnected: code=${event.code} reason="${event.reason}"`);
          this.stopPingInterval();
          this.setConnectionState('disconnected');

          if (!this.manualDisconnect) {
            this.scheduleReconnect();
          }
        };

        this.socket.onerror = (error) => {
          console.error('[WS Client] Error:', error);
          // Don't reject here - onclose will be called
        };

        this.socket.onmessage = (event) => {
          this.handleMessage(event.data);
        };
      } catch (error) {
        this.setConnectionState('disconnected');
        reject(error);
      }
    });
  }

  /**
   * Disconnect from the WebSocket server
   */
  disconnect(): void {
    this.manualDisconnect = true;
    this.cancelReconnect();
    this.stopPingInterval();

    if (this.socket) {
      console.log('[WS Client] Disconnecting');
      this.socket.close(1000, 'Client disconnect');
      this.socket = null;
    }

    this.setConnectionState('disconnected');
  }

  /**
   * Send a message to the server
   */
  send(message: Omit<WebSocketMessage, 'deviceId' | 'vaultName' | 'timestamp'>): boolean {
    if (!this.isConnected()) {
      console.warn('[WS Client] Cannot send - not connected');
      return false;
    }

    const fullMessage: WebSocketMessage = {
      ...message,
      vaultName: this.vaultName,
      deviceId: this.deviceId,
      timestamp: Date.now(),
    };

    try {
      this.socket!.send(JSON.stringify(fullMessage));
      return true;
    } catch (error) {
      console.error('[WS Client] Send error:', error);
      return false;
    }
  }

  /**
   * Register a message handler for a specific message type
   */
  onMessage(type: string, handler: MessageHandler): () => void {
    const handlers = this.messageHandlers.get(type) || [];
    handlers.push(handler);
    this.messageHandlers.set(type, handlers);

    // Return unsubscribe function
    return () => {
      const currentHandlers = this.messageHandlers.get(type) || [];
      const index = currentHandlers.indexOf(handler);
      if (index !== -1) {
        currentHandlers.splice(index, 1);
        this.messageHandlers.set(type, currentHandlers);
      }
    };
  }

  /**
   * Register a connection state change handler
   */
  onConnectionStateChange(handler: ConnectionStateHandler): () => void {
    this.connectionStateHandlers.push(handler);

    // Return unsubscribe function
    return () => {
      const index = this.connectionStateHandlers.indexOf(handler);
      if (index !== -1) {
        this.connectionStateHandlers.splice(index, 1);
      }
    };
  }

  /**
   * Check if connected
   */
  isConnected(): boolean {
    return this.socket?.readyState === WebSocket.OPEN;
  }

  /**
   * Get current connection state
   */
  getConnectionState(): ConnectionState {
    return this.connectionState;
  }

  /**
   * Get device ID
   */
  getDeviceId(): string {
    return this.deviceId;
  }

  /**
   * Update server URL (requires reconnect)
   */
  updateServerUrl(serverUrl: string): void {
    this.serverUrl = serverUrl;
  }

  /**
   * Convert HTTP URL to WebSocket URL
   */
  private getWebSocketUrl(): string {
    let wsUrl = this.serverUrl
      .replace(/^http:/, 'ws:')
      .replace(/^https:/, 'wss:');

    // Remove trailing slash
    wsUrl = wsUrl.replace(/\/$/, '');

    return `${wsUrl}/vault/${encodeURIComponent(this.vaultName)}/ws?deviceId=${encodeURIComponent(this.deviceId)}`;
  }

  /**
   * Handle incoming message
   */
  private handleMessage(data: string): void {
    try {
      const message: WebSocketMessage = JSON.parse(data);

      // Handle pong internally
      if (message.type === 'pong') {
        // Server responded to our ping - connection is alive
        return;
      }

      // Handle ping by responding with pong
      if (message.type === 'ping') {
        this.send({ type: 'pong' });
        return;
      }

      // Dispatch to registered handlers
      const handlers = this.messageHandlers.get(message.type) || [];
      for (const handler of handlers) {
        try {
          handler(message);
        } catch (error) {
          console.error(`[WS Client] Handler error for type "${message.type}":`, error);
        }
      }

      // Also dispatch to wildcard handlers
      const wildcardHandlers = this.messageHandlers.get('*') || [];
      for (const handler of wildcardHandlers) {
        try {
          handler(message);
        } catch (error) {
          console.error('[WS Client] Wildcard handler error:', error);
        }
      }
    } catch (error) {
      console.error('[WS Client] Failed to parse message:', error);
    }
  }

  /**
   * Set connection state and notify handlers
   */
  private setConnectionState(state: ConnectionState): void {
    if (this.connectionState === state) return;

    this.connectionState = state;
    console.log(`[WS Client] Connection state: ${state}`);

    for (const handler of this.connectionStateHandlers) {
      try {
        handler(state);
      } catch (error) {
        console.error('[WS Client] Connection state handler error:', error);
      }
    }
  }

  /**
   * Schedule a reconnection attempt
   */
  private scheduleReconnect(): void {
    if (this.reconnectAttempts >= WebSocketClient.MAX_RECONNECT_ATTEMPTS) {
      console.log('[WS Client] Max reconnect attempts reached');
      return;
    }

    this.cancelReconnect();
    this.setConnectionState('reconnecting');

    // Exponential backoff with jitter
    const delay = Math.min(
      WebSocketClient.BASE_RECONNECT_DELAY * Math.pow(2, this.reconnectAttempts),
      WebSocketClient.MAX_RECONNECT_DELAY
    );
    const jitter = Math.random() * 1000;
    const totalDelay = delay + jitter;

    console.log(`[WS Client] Reconnecting in ${Math.round(totalDelay)}ms (attempt ${this.reconnectAttempts + 1})`);

    this.reconnectTimer = setTimeout(async () => {
      this.reconnectAttempts++;
      try {
        await this.connect();
      } catch (error) {
        console.error('[WS Client] Reconnect failed:', error);
        // scheduleReconnect will be called again from onclose
      }
    }, totalDelay);
  }

  /**
   * Cancel pending reconnection
   */
  private cancelReconnect(): void {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
  }

  /**
   * Start ping interval to keep connection alive
   */
  private startPingInterval(): void {
    this.stopPingInterval();

    this.pingTimer = setInterval(() => {
      if (this.isConnected()) {
        this.send({ type: 'ping' });
      }
    }, WebSocketClient.PING_INTERVAL);
  }

  /**
   * Stop ping interval
   */
  private stopPingInterval(): void {
    if (this.pingTimer) {
      clearInterval(this.pingTimer);
      this.pingTimer = null;
    }
  }

  /**
   * Clean up resources
   */
  destroy(): void {
    this.disconnect();
    this.messageHandlers.clear();
    this.connectionStateHandlers = [];
  }
}

/**
 * Generate a unique device ID
 */
export function generateDeviceId(): string {
  // Use a combination of random values and timestamp for uniqueness
  const array = new Uint8Array(8);
  crypto.getRandomValues(array);
  const random = Array.from(array).map(b => b.toString(16).padStart(2, '0')).join('');
  return `device-${random}`;
}
