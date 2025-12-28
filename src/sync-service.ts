import { App, Notice, TFile, Vault, EventRef } from 'obsidian';

interface FileRecord {
  path: string;
  hash: string;
  commit: string;
  updated_at: number;
}

interface SyncStateEntry {
  hash: string;
  commit: string;
}

interface SyncState {
  [path: string]: SyncStateEntry;
}

export interface ScionSyncSettings {
  serverUrl: string;
  pollInterval: number;
  autoSync: boolean;
  syncOnStartup: boolean;
  conflictMode: 'merge' | 'ask' | 'local' | 'remote';
  debounceInterval: number; // seconds to pause sync after typing
}

export type SyncStatus = 'idle' | 'syncing' | 'success' | 'error';

interface SyncResponse {
  success: boolean;
  commit: string;
  hash: string;
  merged: boolean;
  has_conflicts: boolean;
  merged_content?: string;
}

interface PendingConflict {
  path: string;
  mergedContent: string;
  localContent: string;
  serverCommit: string;
}

export class SyncService {
  private app: App;
  private vault: Vault;
  private settings: ScionSyncSettings;
  private vaultName: string;
  private syncState: SyncState = {};
  private debounceTimers: Map<string, ReturnType<typeof setTimeout>> = new Map();
  private eventRefs: EventRef[] = [];
  private saveDataFn: (data: unknown) => Promise<void>;
  private statusCallback: ((status: SyncStatus, message?: string) => void) | null = null;
  private conflictCallback: ((conflict: PendingConflict) => void) | null = null;
  private isSyncing = false;
  private pollTimer: ReturnType<typeof setInterval> | null = null;
  private lastHeadCommit: string | null = null;
  private pendingConflicts: PendingConflict[] = [];
  private isUserActive = false;
  private activityTimer: ReturnType<typeof setTimeout> | null = null;

  private static readonly DEBOUNCE_MS = 2000;

  constructor(
    app: App,
    settings: ScionSyncSettings,
    vaultName: string,
    syncState: SyncState,
    saveDataFn: (data: unknown) => Promise<void>
  ) {
    this.app = app;
    this.vault = app.vault;
    this.settings = settings;
    this.vaultName = vaultName;
    this.syncState = syncState || {};
    this.saveDataFn = saveDataFn;
    console.log('SyncService: Constructor called', {
      serverUrl: settings.serverUrl,
      vaultName: this.vaultName,
      pollInterval: settings.pollInterval,
      existingSyncStateCount: Object.keys(this.syncState).length,
    });
  }

  /**
   * Get the base URL for vault-specific API calls
   */
  private getVaultBaseUrl(): string {
    return `${this.settings.serverUrl}/vault/${encodeURIComponent(this.vaultName)}`;
  }

  setStatusCallback(callback: (status: SyncStatus, message?: string) => void): void {
    this.statusCallback = callback;
    console.log('SyncService: Status callback registered');
  }

  setConflictCallback(callback: (conflict: PendingConflict) => void): void {
    this.conflictCallback = callback;
    console.log('SyncService: Conflict callback registered');
  }

  private updateStatus(status: SyncStatus, message?: string): void {
    console.log(`SyncService: Status changed to '${status}'`, message ? { message } : '');
    this.statusCallback?.(status, message);
  }

  async initialize(): Promise<void> {
    console.log('SyncService: Initializing...');

    try {
      if (this.settings.syncOnStartup) {
        await this.syncAll();
      }
      this.setupFileWatcher();

      if (this.settings.autoSync) {
        this.startPolling();
      }

      console.log('SyncService: Initialization complete');
    } catch (error) {
      console.error('SyncService: Initialization failed', error);
      new Notice('Scion Sync: Failed to connect to server');
    }
  }

  /**
   * Start polling for changes
   */
  startPolling(): void {
    if (this.pollTimer) {
      console.log('SyncService: Polling already active');
      return;
    }

    const interval = this.settings.pollInterval * 1000;
    console.log(`SyncService: Starting polling every ${this.settings.pollInterval}s`);

    this.pollTimer = setInterval(async () => {
      await this.checkForChanges();
    }, interval);
  }

  /**
   * Stop polling
   */
  stopPolling(): void {
    if (this.pollTimer) {
      clearInterval(this.pollTimer);
      this.pollTimer = null;
      console.log('SyncService: Polling stopped');
    }
  }

  /**
   * Check server for changes (polling)
   */
  async checkForChanges(): Promise<void> {
    // Skip polling if user is actively typing
    if (this.isUserActive) {
      console.log('SyncService: Skipping poll - user is active');
      return;
    }

    if (this.isSyncing) {
      return;
    }

    try {
      const url = `${this.getVaultBaseUrl()}/status?since=${this.lastHeadCommit || ''}`;
      const response = await fetch(url);

      if (!response.ok) {
        console.error('SyncService: Status check failed', response.status);
        return;
      }

      const data = await response.json();

      if (data.has_changes && data.changed_files.length > 0) {
        console.log(`SyncService: Server has ${data.changed_files.length} changed files`);

        // Download changed files in parallel
        await Promise.all(data.changed_files.map((filePath: string) => this.downloadFile(filePath)));

        this.lastHeadCommit = data.head_commit;
        new Notice(`Scion Sync: Downloaded ${data.changed_files.length} file(s)`);
      }
    } catch (error) {
      console.error('SyncService: Polling error', error);
    }
  }

  /**
   * Update settings (called when user changes settings)
   */
  updateSettings(settings: ScionSyncSettings): void {
    const pollIntervalChanged = this.settings.pollInterval !== settings.pollInterval;
    const autoSyncChanged = this.settings.autoSync !== settings.autoSync;

    this.settings = settings;

    if (autoSyncChanged) {
      if (settings.autoSync) {
        this.startPolling();
      } else {
        this.stopPolling();
      }
    } else if (pollIntervalChanged && settings.autoSync) {
      this.stopPolling();
      this.startPolling();
    }
  }

  async syncAll(): Promise<void> {
    if (this.isSyncing) {
      console.log('SyncService: Sync already in progress, skipping');
      return;
    }

    console.log('SyncService: Starting full sync...');
    this.isSyncing = true;
    this.updateStatus('syncing');

    try {
      // Fetch server manifest
      console.log('SyncService: Fetching server manifest...');
      const manifest = await this.fetchManifest();
      const serverFiles = new Map(manifest.files.map((f: FileRecord) => [f.path, f]));
      this.lastHeadCommit = manifest.head_commit;
      console.log(`SyncService: Server has ${serverFiles.size} files, head: ${manifest.head_commit}`);

      // Get local files
      const localFiles = this.vault.getFiles();
      const localPaths = new Set(localFiles.map((f) => f.path));
      console.log(`SyncService: Local vault has ${localFiles.length} files`);

      // Collect operations to perform
      const downloadsNeeded: string[] = [];
      const uploadsNeeded: string[] = [];
      const deletionsNeeded: string[] = [];

      // Cache for computed hashes to avoid redundant computation
      const hashCache = new Map<string, string>();

      // Determine files to download (server has, we don't, or server is newer)
      for (const [serverPath, serverRecord] of serverFiles) {
        const localState = this.syncState[serverPath];

        if (!localPaths.has(serverPath)) {
          // Server has file we don't have
          // Only download if we never tracked it (otherwise it was deleted locally)
          if (!localState) {
            console.log(`SyncService: Scheduling download of new file: ${serverPath}`);
            downloadsNeeded.push(serverPath);
          } else {
            console.log(`SyncService: File ${serverPath} was deleted locally, will be deleted from server`);
            deletionsNeeded.push(serverPath);
          }
        } else if (localState && serverRecord.commit !== localState.commit) {
          // Server has newer version (different commit)
          console.log(`SyncService: Server has different version: ${serverPath}`);
          // Check if we have local changes (use cached hash if available)
          let currentHash = hashCache.get(serverPath);
          if (!currentHash) {
            currentHash = await this.computeLocalHash(serverPath);
            hashCache.set(serverPath, currentHash);
          }
          if (currentHash !== localState.hash) {
            // We have local changes too - need to sync/merge
            console.log(`SyncService: Local changes exist, scheduling upload for merge: ${serverPath}`);
            uploadsNeeded.push(serverPath);
          } else {
            // No local changes, just download
            console.log(`SyncService: Scheduling download of updated file: ${serverPath}`);
            downloadsNeeded.push(serverPath);
          }
        }
      }

      // Determine files to upload (we have, server doesn't, or we have local changes)
      for (const file of localFiles) {
        const serverRecord = serverFiles.get(file.path);
        const localState = this.syncState[file.path];

        if (!serverRecord) {
          // We have file server doesn't have
          console.log(`SyncService: Scheduling upload of new file: ${file.path}`);
          uploadsNeeded.push(file.path);
        } else if (localState) {
          // Check if local file changed since last sync (use cached hash if available)
          let currentHash = hashCache.get(file.path);
          if (!currentHash) {
            currentHash = await this.computeLocalHash(file.path);
            hashCache.set(file.path, currentHash);
          }
          if (currentHash !== localState.hash) {
            console.log(`SyncService: Scheduling upload of modified file: ${file.path}`);
            uploadsNeeded.push(file.path);
          }
        }
      }

      // Determine files to delete (we had them in syncState but no longer have locally)
      for (const syncedPath of Object.keys(this.syncState)) {
        if (!localPaths.has(syncedPath)) {
          // File was deleted locally
          console.log(`SyncService: Scheduling deletion from server: ${syncedPath}`);
          deletionsNeeded.push(syncedPath);
        }
      }

      // Execute operations in parallel
      console.log(`SyncService: Executing ${downloadsNeeded.length} downloads, ${uploadsNeeded.length} uploads, ${deletionsNeeded.length} deletions`);
      await Promise.all([
        ...downloadsNeeded.map((path) => this.downloadFile(path)),
        ...uploadsNeeded.map((path) => this.uploadFile(path)),
        ...deletionsNeeded.map((path) => this.deleteFromServer(path)),
      ]);

      // Remove deleted files from syncState after successful deletion
      for (const deletedPath of deletionsNeeded) {
        delete this.syncState[deletedPath];
      }
      if (deletionsNeeded.length > 0) {
        await this.saveSyncState();
      }

      // Clean up resolved conflicts after sync
      this.clearResolvedConflicts();

      console.log('SyncService: Full sync completed successfully');
      this.updateStatus('success');
      new Notice('Scion Sync: Sync complete');
    } catch (error) {
      console.error('SyncService: Sync failed', error);
      this.updateStatus('error', error instanceof Error ? error.message : 'Unknown error');
      new Notice('Scion Sync: Sync failed - check console for details');
    } finally {
      this.isSyncing = false;
      console.log('SyncService: Sync lock released');
    }
  }

  async uploadFile(path: string): Promise<void> {
    console.log(`SyncService: uploadFile called for: ${path}`);
    const file = this.vault.getAbstractFileByPath(path);
    if (!(file instanceof TFile)) {
      console.warn(`SyncService: Cannot upload, file not found: ${path}`);
      return;
    }

    try {
      // Read file content as binary
      const content = await this.vault.readBinary(file);
      const base64Content = this.arrayBufferToBase64(content);
      console.log(`SyncService: Read file ${path}, size: ${content.byteLength} bytes`);

      // Get base_commit from sync state
      const localState = this.syncState[path];
      const baseCommit = localState?.commit ?? null;
      console.log(`SyncService: Upload ${path} with base_commit: ${baseCommit}`);

      // POST to server
      const response = await fetch(`${this.getVaultBaseUrl()}/sync`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          path,
          content: base64Content,
          base_commit: baseCommit,
        }),
      });

      console.log(`SyncService: Upload response status: ${response.status}`);

      if (!response.ok) {
        const errorText = await response.text();
        console.error(`SyncService: Upload failed for ${path}`, { status: response.status, body: errorText });
        throw new Error(`Upload failed: ${response.status} ${response.statusText}`);
      }

      const result: SyncResponse = await response.json();
      console.log(`SyncService: Upload result for ${path}`, {
        commit: result.commit,
        merged: result.merged,
        has_conflicts: result.has_conflicts,
      });

      // Handle merge result
      if (result.has_conflicts && result.merged_content) {
        await this.handleConflict(path, result);
        return;
      }

      // If merged without conflicts, update local file with merged content
      if (result.merged && result.merged_content) {
        const mergedBuffer = this.base64ToArrayBuffer(result.merged_content);
        await this.vault.modifyBinary(file, mergedBuffer);
        console.log(`SyncService: Updated ${path} with merged content`);
      }

      // Update local sync state
      this.syncState[path] = {
        hash: result.hash,
        commit: result.commit,
      };
      await this.saveSyncState();

      console.log(`SyncService: Uploaded ${path} (commit ${result.commit})`);
    } catch (error) {
      console.error(`SyncService: Failed to upload ${path}`, error);
      throw error;
    }
  }

  /**
   * Delete a file from the server
   */
  private async deleteFromServer(path: string): Promise<boolean> {
    try {
      const url = `${this.getVaultBaseUrl()}/file/${encodeURIComponent(path)}`;
      console.log(`SyncService: Deleting from server: ${path}`);

      const response = await fetch(url, { method: 'DELETE' });

      if (response.ok || response.status === 404) {
        console.log(`SyncService: Deleted ${path} from server`);
        return true;
      }

      console.error(`SyncService: Failed to delete ${path}`, response.status);
      return false;
    } catch (error) {
      console.error(`SyncService: Delete error for ${path}`, error);
      return false;
    }
  }

  private async handleConflict(path: string, result: SyncResponse): Promise<void> {
    console.log(`SyncService: Handling conflict for: ${path}`);

    if (!result.merged_content) {
      console.error('SyncService: No merged content in conflict response');
      return;
    }

    const file = this.vault.getAbstractFileByPath(path);
    if (!(file instanceof TFile)) {
      return;
    }

    // Get local content before overwriting
    const localContent = await this.vault.read(file);
    const mergedContent = Buffer.from(result.merged_content, 'base64').toString('utf-8');

    const conflict: PendingConflict = {
      path,
      mergedContent,
      localContent,
      serverCommit: result.commit,
    };

    // Handle based on conflict mode
    switch (this.settings.conflictMode) {
      case 'ask':
        // Store conflict and notify via callback
        this.pendingConflicts.push(conflict);
        this.conflictCallback?.(conflict);
        new Notice(`Scion Sync: Conflict in "${path}" - please resolve`);
        break;

      case 'local':
        // Keep local version, re-upload
        console.log(`SyncService: Keeping local version for ${path}`);
        // Update sync state to server commit so next upload will try again
        this.syncState[path] = { hash: result.hash, commit: result.commit };
        await this.saveSyncState();
        await this.uploadFile(path);
        break;

      case 'remote':
        // Take server version (download latest)
        console.log(`SyncService: Taking server version for ${path}`);
        await this.downloadFile(path);
        break;

      case 'merge':
      default:
        // Write merged content with conflict markers
        console.log(`SyncService: Writing merged content with markers for ${path}`);
        await this.vault.modify(file, mergedContent);

        // Update sync state - user needs to resolve and save
        this.syncState[path] = {
          hash: result.hash,
          commit: result.commit,
        };
        await this.saveSyncState();

        new Notice(`Scion Sync: Conflict in "${path}" - resolve markers and save`);
        break;
    }
  }

  /**
   * Resolve a pending conflict
   */
  async resolveConflict(path: string, resolution: 'local' | 'remote' | 'merged', content?: string): Promise<void> {
    const file = this.vault.getAbstractFileByPath(path);
    if (!(file instanceof TFile)) {
      return;
    }

    switch (resolution) {
      case 'local':
        // Re-upload local content
        await this.uploadFile(path);
        break;

      case 'remote':
        // Download server version
        await this.downloadFile(path);
        break;

      case 'merged':
        // Write resolved content and upload
        if (content) {
          await this.vault.modify(file, content);
          await this.uploadFile(path);
        }
        break;
    }

    // Remove from pending conflicts
    this.pendingConflicts = this.pendingConflicts.filter((c) => c.path !== path);
  }

  /**
   * Get pending conflicts
   */
  getPendingConflicts(): PendingConflict[] {
    return [...this.pendingConflicts];
  }

  /**
   * Clear resolved conflicts (call after user has addressed them)
   */
  clearResolvedConflicts(): void {
    // Remove conflicts for files that no longer exist or have been synced
    this.pendingConflicts = this.pendingConflicts.filter((conflict) => {
      const file = this.vault.getAbstractFileByPath(conflict.path);
      const stillExists = file instanceof TFile;
      const hasLocalState = !!this.syncState[conflict.path];

      // Keep conflict if file exists and hasn't been synced yet
      return stillExists && hasLocalState;
    });
  }

  async downloadFile(path: string): Promise<void> {
    console.log(`SyncService: downloadFile called for: ${path}`);
    try {
      const url = `${this.getVaultBaseUrl()}/file/${encodeURIComponent(path)}`;
      console.log(`SyncService: Fetching from: ${url}`);
      const response = await fetch(url);

      console.log(`SyncService: Download response status: ${response.status}`);

      if (!response.ok) {
        const errorText = await response.text();
        console.error(`SyncService: Download failed for ${path}`, { status: response.status, body: errorText });
        throw new Error(`Download failed: ${response.status} ${response.statusText}`);
      }

      const content = await response.arrayBuffer();
      const commit = response.headers.get('X-File-Commit') || '';
      const hash = response.headers.get('X-File-Hash') || '';
      console.log(`SyncService: Downloaded ${path}, size: ${content.byteLength} bytes, commit: ${commit}`);

      // Write to vault
      const existingFile = this.vault.getAbstractFileByPath(path);
      if (existingFile instanceof TFile) {
        console.log(`SyncService: Updating existing file: ${path}`);
        await this.vault.modifyBinary(existingFile, content);
      } else {
        // Ensure parent folder exists
        const folderPath = path.substring(0, path.lastIndexOf('/'));
        if (folderPath && !this.vault.getAbstractFileByPath(folderPath)) {
          console.log(`SyncService: Creating parent folder: ${folderPath}`);
          await this.vault.createFolder(folderPath);
        }
        console.log(`SyncService: Creating new file: ${path}`);
        await this.vault.createBinary(path, content);
      }

      // Update local sync state
      this.syncState[path] = { hash, commit };
      await this.saveSyncState();

      console.log(`SyncService: Download complete for ${path} (commit ${commit})`);
    } catch (error) {
      console.error(`SyncService: Failed to download ${path}`, error);
      throw error;
    }
  }

  setupFileWatcher(): void {
    console.log('SyncService: Setting up file watchers...');

    // Watch for file modifications
    const modifyRef = this.vault.on('modify', (file) => {
      if (file instanceof TFile) {
        console.log(`SyncService: File modified event: ${file.path}`);
        this.debouncedUpload(file.path);
      }
    });
    this.eventRefs.push(modifyRef);

    // Watch for file creation
    const createRef = this.vault.on('create', (file) => {
      if (file instanceof TFile) {
        console.log(`SyncService: File created event: ${file.path}`);
        this.debouncedUpload(file.path);
      }
    });
    this.eventRefs.push(createRef);

    // Watch for file deletion
    const deleteRef = this.vault.on('delete', (file) => {
      if (file instanceof TFile) {
        console.log(`SyncService: File deleted event: ${file.path}`);
        this.handleFileDelete(file.path);
      }
    });
    this.eventRefs.push(deleteRef);

    console.log('SyncService: File watcher setup complete (modify, create, delete)');
  }

  /**
   * Mark user as active (typing) - pauses polling until debounce interval passes
   */
  private markUserActive(): void {
    this.isUserActive = true;

    // Clear existing activity timer
    if (this.activityTimer) {
      clearTimeout(this.activityTimer);
    }

    // Resume sync after debounce interval
    const debounceMs = this.settings.debounceInterval * 1000;
    this.activityTimer = setTimeout(() => {
      this.isUserActive = false;
      console.log('SyncService: User idle, resuming sync');
    }, debounceMs);
  }

  private debouncedUpload(path: string): void {
    // Mark user as active (pauses polling)
    this.markUserActive();

    if (!this.settings.autoSync) {
      return;
    }

    // Clear existing timer for this path
    const existingTimer = this.debounceTimers.get(path);
    if (existingTimer) {
      console.log(`SyncService: Debounce reset for: ${path}`);
      clearTimeout(existingTimer);
    } else {
      console.log(`SyncService: Debounce started for: ${path} (${SyncService.DEBOUNCE_MS}ms)`);
    }

    // Set new timer
    const timer = setTimeout(async () => {
      console.log(`SyncService: Debounce timer fired for: ${path}`);
      this.debounceTimers.delete(path);
      try {
        await this.uploadFile(path);
      } catch (error) {
        console.error(`SyncService: Debounced upload failed for ${path}`, error);
      }
    }, SyncService.DEBOUNCE_MS);

    this.debounceTimers.set(path, timer);
  }

  private async handleFileDelete(path: string): Promise<void> {
    console.log(`SyncService: Handling file deletion: ${path}`);

    // Delete from server if we had synced this file before
    if (this.syncState[path]) {
      if (this.settings.autoSync) {
        // Delete immediately if auto-sync is enabled
        await this.deleteFromServer(path);
        console.log(`SyncService: Deleted ${path} from server`);
        // Only remove from sync state after successful deletion
        delete this.syncState[path];
        await this.saveSyncState();
      } else {
        // Manual sync mode: keep the entry in syncState so we can detect deletion during syncAll
        // Mark it with a special tombstone value to indicate deletion
        console.log(`SyncService: File ${path} deleted locally, marking for deletion on next manual sync`);
        // Keep syncState entry but file is gone - syncAll will detect this
      }
    }
  }

  async computeLocalHash(path: string): Promise<string> {
    console.log(`SyncService: Computing hash for: ${path}`);
    const file = this.vault.getAbstractFileByPath(path);
    if (!(file instanceof TFile)) {
      console.error(`SyncService: Cannot compute hash, file not found: ${path}`);
      throw new Error(`File not found: ${path}`);
    }

    const content = await this.vault.readBinary(file);
    const hashBuffer = await crypto.subtle.digest('SHA-256', content);
    const hash = this.arrayBufferToHex(hashBuffer);
    console.log(`SyncService: Hash computed for ${path}: ${hash.substring(0, 16)}...`);
    return hash;
  }

  private async fetchManifest(): Promise<{ files: FileRecord[]; head_commit: string }> {
    const url = `${this.getVaultBaseUrl()}/manifest`;
    console.log(`SyncService: Fetching manifest from: ${url}`);
    const response = await fetch(url);
    if (!response.ok) {
      console.error(`SyncService: Manifest fetch failed, status: ${response.status}`);
      throw new Error(`Failed to fetch manifest: ${response.status}`);
    }
    const manifest = await response.json();
    console.log(`SyncService: Manifest fetched, ${manifest.files?.length || 0} files, head: ${manifest.head_commit}`);
    return manifest;
  }

  private async saveSyncState(): Promise<void> {
    console.log(`SyncService: Saving sync state (${Object.keys(this.syncState).length} entries)`);
    await this.saveDataFn({ syncState: this.syncState });
  }

  private arrayBufferToBase64(buffer: ArrayBuffer): string {
    const bytes = new Uint8Array(buffer);
    let binary = '';
    for (let i = 0; i < bytes.byteLength; i++) {
      binary += String.fromCharCode(bytes[i]);
    }
    return btoa(binary);
  }

  private base64ToArrayBuffer(base64: string): ArrayBuffer {
    const binary = atob(base64);
    const bytes = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) {
      bytes[i] = binary.charCodeAt(i);
    }
    return bytes.buffer;
  }

  private arrayBufferToHex(buffer: ArrayBuffer): string {
    const bytes = new Uint8Array(buffer);
    return Array.from(bytes)
      .map((b) => b.toString(16).padStart(2, '0'))
      .join('');
  }

  /**
   * Get sync statistics
   */
  getStats(): { trackedFiles: number; lastCommit: string | null; pendingConflicts: number } {
    return {
      trackedFiles: Object.keys(this.syncState).length,
      lastCommit: this.lastHeadCommit,
      pendingConflicts: this.pendingConflicts.length,
    };
  }

  destroy(): void {
    console.log('SyncService: Destroying...');

    // Stop polling
    this.stopPolling();

    // Clear activity timer
    if (this.activityTimer) {
      clearTimeout(this.activityTimer);
      this.activityTimer = null;
    }

    // Clear all debounce timers
    const timerCount = this.debounceTimers.size;
    for (const timer of this.debounceTimers.values()) {
      clearTimeout(timer);
    }
    this.debounceTimers.clear();
    console.log(`SyncService: Cleared ${timerCount} debounce timers`);

    // Unregister event listeners
    const refCount = this.eventRefs.length;
    for (const ref of this.eventRefs) {
      this.vault.offref(ref);
    }
    this.eventRefs = [];
    console.log(`SyncService: Unregistered ${refCount} event listeners`);

    console.log('SyncService: Destroyed');
  }
}
