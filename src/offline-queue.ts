/**
 * Persistent Offline Queue
 * Queues operations when offline, persists to plugin data, syncs on reconnect
 */

export interface QueuedOperation {
  id: string;
  type: 'yjs-update' | 'structure-update' | 'binary-sync';
  fileId: string;
  path: string;
  payload: string; // base64 encoded
  timestamp: number;
  retryCount: number;
}

export interface OfflineQueueStatus {
  count: number;
  oldestTimestamp: number | null;
  totalRetries: number;
}

type SaveFn = (queue: QueuedOperation[]) => Promise<void>;
type ProcessFn = (operation: QueuedOperation) => Promise<boolean>;

/**
 * Manages offline operation queue with persistence
 */
export class OfflineQueue {
  private queue: QueuedOperation[] = [];
  private saveFn: SaveFn;
  private processFn: ProcessFn | null = null;
  private isProcessing = false;
  private maxRetries = 3;
  private retryDelayMs = 5000;

  constructor(saveFn: SaveFn) {
    this.saveFn = saveFn;
    console.log('[OfflineQueue] Initialized');
  }

  /**
   * Set the function to process queued operations
   */
  setProcessor(processFn: ProcessFn): void {
    this.processFn = processFn;
  }

  /**
   * Load queue from persisted data
   */
  load(queue: QueuedOperation[]): void {
    this.queue = queue || [];
    console.log(`[OfflineQueue] Loaded ${this.queue.length} operations`);
  }

  /**
   * Generate unique ID for operation
   */
  private generateId(): string {
    return `${Date.now()}-${Math.random().toString(36).slice(2, 11)}`;
  }

  /**
   * Enqueue an operation
   * Deduplicates by fileId and type (replaces older operation)
   */
  enqueue(operation: Omit<QueuedOperation, 'id' | 'timestamp' | 'retryCount'>): void {
    // Remove existing operation for same file and type
    this.queue = this.queue.filter(
      (op) => !(op.fileId === operation.fileId && op.type === operation.type)
    );

    const queuedOp: QueuedOperation = {
      ...operation,
      id: this.generateId(),
      timestamp: Date.now(),
      retryCount: 0,
    };

    this.queue.push(queuedOp);
    console.log(`[OfflineQueue] Enqueued ${operation.type} for ${operation.path} (queue size: ${this.queue.length})`);

    // Persist immediately
    this.persist();
  }

  /**
   * Remove an operation from the queue
   */
  remove(id: string): void {
    const before = this.queue.length;
    this.queue = this.queue.filter((op) => op.id !== id);
    if (this.queue.length < before) {
      console.log(`[OfflineQueue] Removed operation ${id}`);
      this.persist();
    }
  }

  /**
   * Get all queued operations for a file
   */
  getOperationsForFile(fileId: string): QueuedOperation[] {
    return this.queue.filter((op) => op.fileId === fileId);
  }

  /**
   * Check if there are pending operations for a file
   */
  hasPendingForFile(fileId: string): boolean {
    return this.queue.some((op) => op.fileId === fileId);
  }

  /**
   * Process all queued operations
   * Called when coming back online
   */
  async processQueue(): Promise<{ processed: number; failed: number }> {
    if (this.isProcessing) {
      console.log('[OfflineQueue] Already processing');
      return { processed: 0, failed: 0 };
    }

    if (!this.processFn) {
      console.warn('[OfflineQueue] No processor set');
      return { processed: 0, failed: 0 };
    }

    if (this.queue.length === 0) {
      console.log('[OfflineQueue] Queue is empty');
      return { processed: 0, failed: 0 };
    }

    this.isProcessing = true;
    console.log(`[OfflineQueue] Processing ${this.queue.length} operations...`);

    let processed = 0;
    let failed = 0;

    // Process oldest first
    const sortedQueue = [...this.queue].sort((a, b) => a.timestamp - b.timestamp);

    for (const operation of sortedQueue) {
      try {
        const success = await this.processFn(operation);

        if (success) {
          this.remove(operation.id);
          processed++;
          console.log(`[OfflineQueue] Processed ${operation.type} for ${operation.path}`);
        } else {
          // Increment retry count
          operation.retryCount++;

          if (operation.retryCount >= this.maxRetries) {
            console.warn(`[OfflineQueue] Max retries reached for ${operation.path}, removing`);
            this.remove(operation.id);
            failed++;
          } else {
            console.log(`[OfflineQueue] Retry ${operation.retryCount}/${this.maxRetries} for ${operation.path}`);
          }
        }
      } catch (error) {
        console.error(`[OfflineQueue] Error processing ${operation.path}:`, error);
        operation.retryCount++;

        if (operation.retryCount >= this.maxRetries) {
          this.remove(operation.id);
          failed++;
        }
      }

      // Small delay between operations to avoid overwhelming the server
      await new Promise((resolve) => setTimeout(resolve, 100));
    }

    this.isProcessing = false;
    await this.persist();

    console.log(`[OfflineQueue] Processing complete: ${processed} processed, ${failed} failed`);
    return { processed, failed };
  }

  /**
   * Persist queue to storage
   */
  async persist(): Promise<void> {
    try {
      await this.saveFn(this.queue);
      console.log(`[OfflineQueue] Persisted ${this.queue.length} operations`);
    } catch (error) {
      console.error('[OfflineQueue] Failed to persist:', error);
    }
  }

  /**
   * Get queue status
   */
  getStatus(): OfflineQueueStatus {
    const oldestTimestamp = this.queue.length > 0
      ? Math.min(...this.queue.map((op) => op.timestamp))
      : null;

    const totalRetries = this.queue.reduce((sum, op) => sum + op.retryCount, 0);

    return {
      count: this.queue.length,
      oldestTimestamp,
      totalRetries,
    };
  }

  /**
   * Get all queued operations (for debugging)
   */
  getAll(): QueuedOperation[] {
    return [...this.queue];
  }

  /**
   * Clear all operations
   */
  clear(): void {
    this.queue = [];
    this.persist();
    console.log('[OfflineQueue] Cleared');
  }

  /**
   * Check if queue is being processed
   */
  isProcessingQueue(): boolean {
    return this.isProcessing;
  }

  /**
   * Get queue size
   */
  size(): number {
    return this.queue.length;
  }
}

/**
 * Format time since timestamp for display
 */
export function formatTimeSince(timestamp: number): string {
  const seconds = Math.floor((Date.now() - timestamp) / 1000);

  if (seconds < 60) {
    return `${seconds}s ago`;
  }

  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) {
    return `${minutes}m ago`;
  }

  const hours = Math.floor(minutes / 60);
  if (hours < 24) {
    return `${hours}h ago`;
  }

  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}
