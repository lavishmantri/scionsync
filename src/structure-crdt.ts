import * as Y from 'yjs';

/**
 * Structure CRDT - Manages file/folder structure using Yjs Y.Map
 * Uses tombstone pattern for deletions (marked deleted, not removed)
 * Syncs via WebSocket to server and other clients
 */

export interface FileEntry {
  file_id: string;
  path: string;
  type: 'file' | 'folder';
  deleted: boolean;
  created_at: number;
  modified_at: number;
  hash?: string;
}

export interface StructureUpdate {
  type: 'create' | 'delete' | 'rename' | 'move';
  file_id: string;
  path: string;
  old_path?: string;
  entry_type?: 'file' | 'folder';
}

/**
 * Manages structure CRDT on the plugin side
 */
export class StructureCRDT {
  private doc: Y.Doc;
  private files: Y.Map<FileEntry>;
  private updateCallback: ((update: Uint8Array) => void) | null = null;

  constructor() {
    this.doc = new Y.Doc();
    this.files = this.doc.getMap('files');

    // Listen for local changes to broadcast
    this.doc.on('update', (update: Uint8Array, origin: unknown) => {
      // Only broadcast if change originated locally (not from remote)
      if (origin !== 'remote') {
        this.updateCallback?.(update);
      }
    });

    console.log('[StructureCRDT] Initialized');
  }

  /**
   * Set callback for local updates (to send via WebSocket)
   */
  onUpdate(callback: (update: Uint8Array) => void): void {
    this.updateCallback = callback;
  }

  /**
   * Get the current state vector for sync
   */
  getStateVector(): Uint8Array {
    return Y.encodeStateVector(this.doc);
  }

  /**
   * Get the full state for initial sync
   */
  getFullState(): Uint8Array {
    return Y.encodeStateAsUpdate(this.doc);
  }

  /**
   * Apply an update from server/other client
   */
  applyRemoteUpdate(update: Uint8Array): void {
    Y.applyUpdate(this.doc, update, 'remote');
    console.log(`[StructureCRDT] Applied remote update, now have ${this.files.size} entries`);
  }

  /**
   * Initialize from server state (full sync)
   */
  initFromServerState(serverState: Uint8Array): void {
    // Clear existing and apply server state
    this.doc.transact(() => {
      this.files.clear();
    });
    Y.applyUpdate(this.doc, serverState, 'remote');
    console.log(`[StructureCRDT] Initialized from server state with ${this.files.size} entries`);
  }

  /**
   * Add a new file to the structure (local change)
   */
  addFile(fileId: string, filePath: string, entryType: 'file' | 'folder', hash?: string): FileEntry {
    const now = Date.now();
    const entry: FileEntry = {
      file_id: fileId,
      path: filePath,
      type: entryType,
      deleted: false,
      created_at: now,
      modified_at: now,
      hash,
    };

    this.doc.transact(() => {
      this.files.set(fileId, entry);
    });

    console.log(`[StructureCRDT] Added ${entryType}: "${filePath}" (${fileId})`);
    return entry;
  }

  /**
   * Mark a file as deleted (tombstone pattern)
   */
  deleteFile(fileId: string): boolean {
    const entry = this.files.get(fileId);
    if (!entry) {
      console.warn(`[StructureCRDT] Cannot delete, file not found: ${fileId}`);
      return false;
    }

    const updatedEntry: FileEntry = {
      ...entry,
      deleted: true,
      modified_at: Date.now(),
    };

    this.doc.transact(() => {
      this.files.set(fileId, updatedEntry);
    });

    console.log(`[StructureCRDT] Deleted: "${entry.path}" (${fileId})`);
    return true;
  }

  /**
   * Rename/move a file
   */
  renameFile(fileId: string, newPath: string): boolean {
    const entry = this.files.get(fileId);
    if (!entry) {
      console.warn(`[StructureCRDT] Cannot rename, file not found: ${fileId}`);
      return false;
    }

    const oldPath = entry.path;
    const updatedEntry: FileEntry = {
      ...entry,
      path: newPath,
      modified_at: Date.now(),
    };

    this.doc.transact(() => {
      this.files.set(fileId, updatedEntry);
    });

    console.log(`[StructureCRDT] Renamed: "${oldPath}" -> "${newPath}" (${fileId})`);
    return true;
  }

  /**
   * Update file hash (when content changes)
   */
  updateFileHash(fileId: string, hash: string): boolean {
    const entry = this.files.get(fileId);
    if (!entry) {
      return false;
    }

    const updatedEntry: FileEntry = {
      ...entry,
      hash,
      modified_at: Date.now(),
    };

    this.doc.transact(() => {
      this.files.set(fileId, updatedEntry);
    });

    return true;
  }

  /**
   * Get a file entry by ID
   */
  getFile(fileId: string): FileEntry | undefined {
    return this.files.get(fileId);
  }

  /**
   * Get a file entry by path (searches non-deleted files)
   */
  getFileByPath(filePath: string): FileEntry | undefined {
    for (const entry of this.files.values()) {
      if (!entry.deleted && entry.path === filePath) {
        return entry;
      }
    }
    return undefined;
  }

  /**
   * Get file ID by path
   */
  getFileIdByPath(filePath: string): string | undefined {
    const entry = this.getFileByPath(filePath);
    return entry?.file_id;
  }

  /**
   * Get all non-deleted files
   */
  getActiveFiles(): FileEntry[] {
    const active: FileEntry[] = [];
    for (const entry of this.files.values()) {
      if (!entry.deleted) {
        active.push(entry);
      }
    }
    return active;
  }

  /**
   * Check if a file exists (non-deleted)
   */
  hasFile(fileId: string): boolean {
    const entry = this.files.get(fileId);
    return entry !== undefined && !entry.deleted;
  }

  /**
   * Check if a path exists (non-deleted)
   */
  hasPath(filePath: string): boolean {
    return this.getFileByPath(filePath) !== undefined;
  }

  /**
   * Get files that need to be created locally (exist in CRDT but not locally)
   */
  getFilesToCreate(localPaths: Set<string>): FileEntry[] {
    const toCreate: FileEntry[] = [];
    for (const entry of this.files.values()) {
      if (!entry.deleted && entry.type === 'file' && !localPaths.has(entry.path)) {
        toCreate.push(entry);
      }
    }
    return toCreate;
  }

  /**
   * Get files that need to be deleted locally (marked deleted in CRDT)
   */
  getFilesToDelete(localPaths: Set<string>): FileEntry[] {
    const toDelete: FileEntry[] = [];
    for (const entry of this.files.values()) {
      if (entry.deleted && localPaths.has(entry.path)) {
        toDelete.push(entry);
      }
    }
    return toDelete;
  }

  /**
   * Get files that need to be renamed locally
   */
  getFilesToRename(localPathsToFileId: Map<string, string>): Array<{ entry: FileEntry; localPath: string }> {
    const toRename: Array<{ entry: FileEntry; localPath: string }> = [];
    for (const [localPath, fileId] of localPathsToFileId) {
      const entry = this.files.get(fileId);
      if (entry && !entry.deleted && entry.path !== localPath) {
        toRename.push({ entry, localPath });
      }
    }
    return toRename;
  }

  /**
   * Clean up resources
   */
  destroy(): void {
    this.doc.destroy();
    this.updateCallback = null;
    console.log('[StructureCRDT] Destroyed');
  }
}

/**
 * Convert Uint8Array to base64 string
 */
export function structureUint8ArrayToBase64(arr: Uint8Array): string {
  let binary = '';
  for (let i = 0; i < arr.length; i++) {
    binary += String.fromCharCode(arr[i]);
  }
  return btoa(binary);
}

/**
 * Convert base64 string to Uint8Array
 */
export function structureBase64ToUint8Array(base64: string): Uint8Array {
  const binary = atob(base64);
  const arr = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) {
    arr[i] = binary.charCodeAt(i);
  }
  return arr;
}
