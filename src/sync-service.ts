import { App, Notice, TFile, Vault, EventRef } from 'obsidian';

interface FileRecord {
  path: string;
  hash: string;
  revision: number;
  updated_at: number;
}

interface SyncStateEntry {
  hash: string;
  revision: number;
}

interface SyncState {
  [path: string]: SyncStateEntry;
}

interface ScionSyncSettings {
  serverUrl: string;
}

export type SyncStatus = 'idle' | 'syncing' | 'success' | 'error';

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
  private isSyncing = false;

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

  private updateStatus(status: SyncStatus, message?: string): void {
    console.log(`SyncService: Status changed to '${status}'`, message ? { message } : '');
    this.statusCallback?.(status, message);
  }

  async initialize(): Promise<void> {
    console.log('SyncService: Initializing...');

    try {
      await this.syncAll();
      this.setupFileWatcher();
      console.log('SyncService: Initialization complete');
    } catch (error) {
      console.error('SyncService: Initialization failed', error);
      new Notice('Scion Sync: Failed to connect to server');
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
      const serverFiles = new Map(manifest.files.map((f) => [f.path, f]));
      console.log(`SyncService: Server has ${serverFiles.size} files`);

      // Get local files
      const localFiles = this.vault.getFiles();
      const localPaths = new Set(localFiles.map((f) => f.path));
      console.log(`SyncService: Local vault has ${localFiles.length} files`);

      // Files to download (server has, we don't, or server is newer)
      for (const [serverPath, serverRecord] of serverFiles) {
        const localState = this.syncState[serverPath];

        if (!localPaths.has(serverPath)) {
          // Server has file we don't have
          console.log(`SyncService: Downloading new file: ${serverPath}`);
          await this.downloadFile(serverPath);
        } else if (localState && serverRecord.revision > localState.revision) {
          // Server has newer version
          console.log(`SyncService: Downloading updated file: ${serverPath}`);
          await this.downloadFile(serverPath);
        }
      }

      // Files to upload (we have, server doesn't, or we have local changes)
      for (const file of localFiles) {
        const serverRecord = serverFiles.get(file.path);
        const localState = this.syncState[file.path];

        if (!serverRecord) {
          // We have file server doesn't have
          console.log(`SyncService: Uploading new file: ${file.path}`);
          await this.uploadFile(file.path);
        } else if (localState) {
          // Check if local file changed since last sync
          const currentHash = await this.computeLocalHash(file.path);
          if (currentHash !== localState.hash) {
            console.log(`SyncService: Uploading modified file: ${file.path}`);
            await this.uploadFile(file.path);
          }
        }
      }

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

      // Get current revision from sync state
      const localState = this.syncState[path];
      const clientRevision = localState?.revision ?? null;
      console.log(`SyncService: Upload ${path} with client_revision: ${clientRevision}`);

      // POST to server
      const response = await fetch(`${this.getVaultBaseUrl()}/sync`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          path,
          content: base64Content,
          client_revision: clientRevision,
        }),
      });

      console.log(`SyncService: Upload response status: ${response.status}`);

      if (response.status === 409) {
        // Conflict - server has newer version
        const data = await response.json();
        console.warn(`SyncService: Conflict detected for ${path}`, data);
        await this.handleConflict(path);
        return;
      }

      if (!response.ok) {
        const errorText = await response.text();
        console.error(`SyncService: Upload failed for ${path}`, { status: response.status, body: errorText });
        throw new Error(`Upload failed: ${response.status} ${response.statusText}`);
      }

      const result = await response.json();
      console.log(`SyncService: Upload successful for ${path}`, { hash: result.hash, revision: result.revision });

      // Update local sync state
      this.syncState[path] = {
        hash: result.hash,
        revision: result.revision,
      };
      await this.saveSyncState();

      console.log(`SyncService: Uploaded ${path} (revision ${result.revision})`);
    } catch (error) {
      console.error(`SyncService: Failed to upload ${path}`, error);
      throw error;
    }
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
      const revision = parseInt(response.headers.get('X-File-Revision') || '1', 10);
      const hash = response.headers.get('X-File-Hash') || '';
      console.log(`SyncService: Downloaded ${path}, size: ${content.byteLength} bytes, revision: ${revision}`);

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
      this.syncState[path] = { hash, revision };
      await this.saveSyncState();

      console.log(`SyncService: Download complete for ${path} (revision ${revision})`);
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

  private debouncedUpload(path: string): void {
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
    // Remove from sync state
    const hadState = !!this.syncState[path];
    delete this.syncState[path];
    await this.saveSyncState();
    console.log(`SyncService: File ${path} removed from sync state (had previous state: ${hadState})`);
    // TODO: Optionally delete from server via DELETE /file/*
  }

  private async handleConflict(originalPath: string): Promise<void> {
    console.log(`SyncService: Handling conflict for: ${originalPath}`);
    try {
      // Generate conflict file path: "notes/file.md" → "notes/file (Conflict).md"
      const lastDot = originalPath.lastIndexOf('.');
      const ext = lastDot !== -1 ? originalPath.substring(lastDot) : '';
      const base = lastDot !== -1 ? originalPath.substring(0, lastDot) : originalPath;
      const conflictPath = `${base} (Conflict)${ext}`;
      console.log(`SyncService: Conflict file will be saved as: ${conflictPath}`);

      // Download server version
      const url = `${this.getVaultBaseUrl()}/file/${encodeURIComponent(originalPath)}`;
      console.log(`SyncService: Downloading server version from: ${url}`);
      const response = await fetch(url);

      if (!response.ok) {
        console.error(`SyncService: Failed to download conflict file, status: ${response.status}`);
        throw new Error(`Failed to download conflict file: ${response.status}`);
      }

      const content = await response.arrayBuffer();
      const revision = parseInt(response.headers.get('X-File-Revision') || '1', 10);
      const hash = response.headers.get('X-File-Hash') || '';
      console.log(`SyncService: Server version downloaded, size: ${content.byteLength}, revision: ${revision}`);

      // Save as conflict file
      const existingConflict = this.vault.getAbstractFileByPath(conflictPath);
      if (existingConflict instanceof TFile) {
        console.log(`SyncService: Updating existing conflict file: ${conflictPath}`);
        await this.vault.modifyBinary(existingConflict, content);
      } else {
        // Ensure parent folder exists
        const folderPath = conflictPath.substring(0, conflictPath.lastIndexOf('/'));
        if (folderPath && !this.vault.getAbstractFileByPath(folderPath)) {
          console.log(`SyncService: Creating parent folder for conflict: ${folderPath}`);
          await this.vault.createFolder(folderPath);
        }
        console.log(`SyncService: Creating conflict file: ${conflictPath}`);
        await this.vault.createBinary(conflictPath, content);
      }

      // Update sync state for original path to server's revision
      // This marks that we're now aware of the server's version
      this.syncState[originalPath] = { hash, revision };
      await this.saveSyncState();
      console.log(`SyncService: Updated sync state for ${originalPath} to revision ${revision}`);

      // Notify user
      const fileName = originalPath.substring(originalPath.lastIndexOf('/') + 1);
      new Notice(`Scion Sync: Conflict in "${fileName}". Remote version saved as "${fileName.replace(ext, ` (Conflict)${ext}`)}"`);

      console.log(`SyncService: Conflict resolved for ${originalPath} → ${conflictPath}`);
    } catch (error) {
      console.error(`SyncService: Failed to handle conflict for ${originalPath}`, error);
      new Notice(`Scion Sync: Failed to resolve conflict for ${originalPath}`);
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

  private async fetchManifest(): Promise<{ files: FileRecord[] }> {
    const url = `${this.getVaultBaseUrl()}/manifest`;
    console.log(`SyncService: Fetching manifest from: ${url}`);
    const response = await fetch(url);
    if (!response.ok) {
      console.error(`SyncService: Manifest fetch failed, status: ${response.status}`);
      throw new Error(`Failed to fetch manifest: ${response.status}`);
    }
    const manifest = await response.json();
    console.log(`SyncService: Manifest fetched, ${manifest.files?.length || 0} files`);
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

  private arrayBufferToHex(buffer: ArrayBuffer): string {
    const bytes = new Uint8Array(buffer);
    return Array.from(bytes)
      .map((b) => b.toString(16).padStart(2, '0'))
      .join('');
  }

  destroy(): void {
    console.log('SyncService: Destroying...');

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
