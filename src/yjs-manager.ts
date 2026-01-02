import * as Y from 'yjs';

export interface YjsDocumentInfo {
  fileId: string;
  doc: Y.Doc;
  lastSyncedStateVector: Uint8Array | null;
  pendingUpdates: Uint8Array[];
  isDirty: boolean;
}

/**
 * Manages Yjs documents for real-time collaborative editing
 * Each file gets its own Y.Doc with a Y.Text for content
 */
export class YjsManager {
  private docs: Map<string, YjsDocumentInfo> = new Map(); // fileId -> YjsDocumentInfo

  constructor() {
    console.log('[YjsManager] Initialized');
  }

  /**
   * Get or create a Y.Doc for a file
   */
  getOrCreateDoc(fileId: string, initialContent?: string): Y.Doc {
    let info = this.docs.get(fileId);

    if (!info) {
      const doc = new Y.Doc();

      // Initialize with content if provided
      if (initialContent !== undefined) {
        const text = doc.getText('content');
        text.insert(0, initialContent);
      }

      info = {
        fileId,
        doc,
        lastSyncedStateVector: null,
        pendingUpdates: [],
        isDirty: false,
      };

      this.docs.set(fileId, info);
      console.log(`[YjsManager] Created new Y.Doc for file="${fileId}"`);
    }

    return info.doc;
  }

  /**
   * Get an existing Y.Doc (doesn't create if missing)
   */
  getDoc(fileId: string): Y.Doc | null {
    const info = this.docs.get(fileId);
    return info?.doc ?? null;
  }

  /**
   * Check if a document exists for a file
   */
  hasDoc(fileId: string): boolean {
    return this.docs.has(fileId);
  }

  /**
   * Apply local changes to a document and return the update
   * Used when the user saves a file locally
   */
  applyLocalChange(fileId: string, newContent: string): Uint8Array | null {
    const info = this.docs.get(fileId);

    if (!info) {
      // Create a new document with the content
      const doc = this.getOrCreateDoc(fileId, newContent);
      info!.isDirty = true;
      return Y.encodeStateAsUpdate(doc);
    }

    const doc = info.doc;
    const text = doc.getText('content');
    const currentContent = text.toString();

    // If content is the same, no update needed
    if (currentContent === newContent) {
      return null;
    }

    // Replace all content with new content
    // In a more sophisticated implementation, we would compute a diff
    // and apply minimal operations
    doc.transact(() => {
      text.delete(0, text.length);
      text.insert(0, newContent);
    });

    info.isDirty = true;

    // Return the update that should be sent to the server
    if (info.lastSyncedStateVector) {
      // Only send the changes since last sync
      return Y.encodeStateAsUpdate(doc, info.lastSyncedStateVector);
    } else {
      // Send full state
      return Y.encodeStateAsUpdate(doc);
    }
  }

  /**
   * Apply a remote update received from the server
   * Returns the new content to write to the file
   */
  applyRemoteUpdate(fileId: string, update: Uint8Array): string {
    const doc = this.getOrCreateDoc(fileId);
    const info = this.docs.get(fileId)!;

    // Apply the remote update
    Y.applyUpdate(doc, update);

    // Update state vector
    info.lastSyncedStateVector = Y.encodeStateVector(doc);
    info.isDirty = false;

    // Return the merged content
    return doc.getText('content').toString();
  }

  /**
   * Initialize a document from server state
   * Called when first syncing a file
   */
  initFromServerState(fileId: string, serverState: Uint8Array): string {
    // Remove existing doc if any
    this.docs.delete(fileId);

    const doc = new Y.Doc();
    Y.applyUpdate(doc, serverState);

    const info: YjsDocumentInfo = {
      fileId,
      doc,
      lastSyncedStateVector: Y.encodeStateVector(doc),
      pendingUpdates: [],
      isDirty: false,
    };

    this.docs.set(fileId, info);
    console.log(`[YjsManager] Initialized doc from server state for file="${fileId}"`);

    return doc.getText('content').toString();
  }

  /**
   * Get the current state vector for a document
   * Used to request only the updates we don't have
   */
  getStateVector(fileId: string): Uint8Array | null {
    const info = this.docs.get(fileId);
    if (!info) return null;

    return Y.encodeStateVector(info.doc);
  }

  /**
   * Get the full state of a document
   */
  getFullState(fileId: string): Uint8Array | null {
    const info = this.docs.get(fileId);
    if (!info) return null;

    return Y.encodeStateAsUpdate(info.doc);
  }

  /**
   * Get the current content of a document
   */
  getContent(fileId: string): string | null {
    const info = this.docs.get(fileId);
    if (!info) return null;

    return info.doc.getText('content').toString();
  }

  /**
   * Mark a document as synced (update the state vector)
   */
  markSynced(fileId: string): void {
    const info = this.docs.get(fileId);
    if (!info) return;

    info.lastSyncedStateVector = Y.encodeStateVector(info.doc);
    info.isDirty = false;
    info.pendingUpdates = [];
  }

  /**
   * Queue an update for later sync (used when offline)
   */
  queueUpdate(fileId: string, update: Uint8Array): void {
    const info = this.docs.get(fileId);
    if (!info) return;

    info.pendingUpdates.push(update);
    console.log(`[YjsManager] Queued update for file="${fileId}", queue size=${info.pendingUpdates.length}`);
  }

  /**
   * Get all pending updates for a file
   */
  getPendingUpdates(fileId: string): Uint8Array[] {
    const info = this.docs.get(fileId);
    if (!info) return [];

    return [...info.pendingUpdates];
  }

  /**
   * Clear pending updates (after successful sync)
   */
  clearPendingUpdates(fileId: string): void {
    const info = this.docs.get(fileId);
    if (!info) return;

    info.pendingUpdates = [];
  }

  /**
   * Check if a document has unsent updates
   */
  isDirty(fileId: string): boolean {
    const info = this.docs.get(fileId);
    return info?.isDirty ?? false;
  }

  /**
   * Remove a document (when file is deleted)
   */
  removeDoc(fileId: string): void {
    const info = this.docs.get(fileId);
    if (info) {
      info.doc.destroy();
      this.docs.delete(fileId);
      console.log(`[YjsManager] Removed Y.Doc for file="${fileId}"`);
    }
  }

  /**
   * Get all document file IDs
   */
  getAllFileIds(): string[] {
    return Array.from(this.docs.keys());
  }

  /**
   * Get all dirty documents that need syncing
   */
  getDirtyDocs(): string[] {
    const dirty: string[] = [];
    for (const [fileId, info] of this.docs) {
      if (info.isDirty || info.pendingUpdates.length > 0) {
        dirty.push(fileId);
      }
    }
    return dirty;
  }

  /**
   * Clean up all documents
   */
  destroy(): void {
    for (const [fileId, info] of this.docs) {
      info.doc.destroy();
    }
    this.docs.clear();
    console.log('[YjsManager] Destroyed');
  }
}

/**
 * Check if a file should use Yjs (text files only)
 */
export function shouldUseYjs(filePath: string): boolean {
  const textExtensions = [
    '.md', '.txt', '.json', '.yaml', '.yml', '.xml', '.html', '.htm',
    '.css', '.scss', '.less', '.js', '.ts', '.jsx', '.tsx',
    '.py', '.rb', '.php', '.java', '.c', '.cpp', '.h', '.hpp',
    '.go', '.rs', '.swift', '.kt', '.scala', '.sh', '.bash',
    '.sql', '.graphql', '.toml', '.ini', '.cfg', '.conf',
    '.csv', '.log', '.markdown', '.mdown', '.mkdn'
  ];

  const ext = filePath.toLowerCase().substring(filePath.lastIndexOf('.'));
  return textExtensions.includes(ext);
}

/**
 * Convert Uint8Array to base64 string
 */
export function uint8ArrayToBase64(arr: Uint8Array): string {
  let binary = '';
  for (let i = 0; i < arr.length; i++) {
    binary += String.fromCharCode(arr[i]);
  }
  return btoa(binary);
}

/**
 * Convert base64 string to Uint8Array
 */
export function base64ToUint8Array(base64: string): Uint8Array {
  const binary = atob(base64);
  const arr = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) {
    arr[i] = binary.charCodeAt(i);
  }
  return arr;
}
