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

export class SyncService {
  private app: App;
  private vault: Vault;
  private settings: ScionSyncSettings;
  private syncState: SyncState = {};
  private debounceTimers: Map<string, ReturnType<typeof setTimeout>> = new Map();
  private eventRefs: EventRef[] = [];
  private saveDataFn: (data: unknown) => Promise<void>;
  private isSyncing = false;

  private static readonly DEBOUNCE_MS = 2000;

  constructor(
    app: App,
    settings: ScionSyncSettings,
    syncState: SyncState,
    saveDataFn: (data: unknown) => Promise<void>
  ) {
    this.app = app;
    this.vault = app.vault;
    this.settings = settings;
    this.syncState = syncState || {};
    this.saveDataFn = saveDataFn;
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

    this.isSyncing = true;

    try {
      // Fetch server manifest
      const manifest = await this.fetchManifest();
      const serverFiles = new Map(manifest.files.map((f) => [f.path, f]));

      // Get local files
      const localFiles = this.vault.getFiles();
      const localPaths = new Set(localFiles.map((f) => f.path));

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

      new Notice('Scion Sync: Sync complete');
    } catch (error) {
      console.error('SyncService: Sync failed', error);
      new Notice('Scion Sync: Sync failed - check console for details');
    } finally {
      this.isSyncing = false;
    }
  }

  async uploadFile(path: string): Promise<void> {
    const file = this.vault.getAbstractFileByPath(path);
    if (!(file instanceof TFile)) {
      console.warn(`SyncService: Cannot upload, file not found: ${path}`);
      return;
    }

    try {
      // Read file content as binary
      const content = await this.vault.readBinary(file);
      const base64Content = this.arrayBufferToBase64(content);

      // Get current revision from sync state
      const localState = this.syncState[path];
      const clientRevision = localState?.revision ?? null;

      // POST to server
      const response = await fetch(`${this.settings.serverUrl}/sync`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          path,
          content: base64Content,
          client_revision: clientRevision,
        }),
      });

      if (response.status === 409) {
        // Conflict - server has newer version
        const data = await response.json();
        console.warn(`SyncService: Conflict detected for ${path}`, data);
        await this.handleConflict(path);
        return;
      }

      if (!response.ok) {
        throw new Error(`Upload failed: ${response.status} ${response.statusText}`);
      }

      const result = await response.json();

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
    try {
      const response = await fetch(`${this.settings.serverUrl}/file/${encodeURIComponent(path)}`);

      if (!response.ok) {
        throw new Error(`Download failed: ${response.status} ${response.statusText}`);
      }

      const content = await response.arrayBuffer();
      const revision = parseInt(response.headers.get('X-File-Revision') || '1', 10);
      const hash = response.headers.get('X-File-Hash') || '';

      // Write to vault
      const existingFile = this.vault.getAbstractFileByPath(path);
      if (existingFile instanceof TFile) {
        await this.vault.modifyBinary(existingFile, content);
      } else {
        // Ensure parent folder exists
        const folderPath = path.substring(0, path.lastIndexOf('/'));
        if (folderPath && !this.vault.getAbstractFileByPath(folderPath)) {
          await this.vault.createFolder(folderPath);
        }
        await this.vault.createBinary(path, content);
      }

      // Update local sync state
      this.syncState[path] = { hash, revision };
      await this.saveSyncState();

      console.log(`SyncService: Downloaded ${path} (revision ${revision})`);
    } catch (error) {
      console.error(`SyncService: Failed to download ${path}`, error);
      throw error;
    }
  }

  setupFileWatcher(): void {
    // Watch for file modifications
    const modifyRef = this.vault.on('modify', (file) => {
      if (file instanceof TFile) {
        this.debouncedUpload(file.path);
      }
    });
    this.eventRefs.push(modifyRef);

    // Watch for file creation
    const createRef = this.vault.on('create', (file) => {
      if (file instanceof TFile) {
        this.debouncedUpload(file.path);
      }
    });
    this.eventRefs.push(createRef);

    // Watch for file deletion
    const deleteRef = this.vault.on('delete', (file) => {
      if (file instanceof TFile) {
        this.handleFileDelete(file.path);
      }
    });
    this.eventRefs.push(deleteRef);

    console.log('SyncService: File watcher setup complete');
  }

  private debouncedUpload(path: string): void {
    // Clear existing timer for this path
    const existingTimer = this.debounceTimers.get(path);
    if (existingTimer) {
      clearTimeout(existingTimer);
    }

    // Set new timer
    const timer = setTimeout(async () => {
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
    console.log(`SyncService: File deleted: ${path}`);
    // Remove from sync state
    delete this.syncState[path];
    await this.saveSyncState();
    // TODO: Optionally delete from server via DELETE /file/*
  }

  private async handleConflict(originalPath: string): Promise<void> {
    try {
      // Generate conflict file path: "notes/file.md" → "notes/file (Conflict).md"
      const lastDot = originalPath.lastIndexOf('.');
      const ext = lastDot !== -1 ? originalPath.substring(lastDot) : '';
      const base = lastDot !== -1 ? originalPath.substring(0, lastDot) : originalPath;
      const conflictPath = `${base} (Conflict)${ext}`;

      // Download server version
      const response = await fetch(
        `${this.settings.serverUrl}/file/${encodeURIComponent(originalPath)}`
      );

      if (!response.ok) {
        throw new Error(`Failed to download conflict file: ${response.status}`);
      }

      const content = await response.arrayBuffer();
      const revision = parseInt(response.headers.get('X-File-Revision') || '1', 10);
      const hash = response.headers.get('X-File-Hash') || '';

      // Save as conflict file
      const existingConflict = this.vault.getAbstractFileByPath(conflictPath);
      if (existingConflict instanceof TFile) {
        await this.vault.modifyBinary(existingConflict, content);
      } else {
        // Ensure parent folder exists
        const folderPath = conflictPath.substring(0, conflictPath.lastIndexOf('/'));
        if (folderPath && !this.vault.getAbstractFileByPath(folderPath)) {
          await this.vault.createFolder(folderPath);
        }
        await this.vault.createBinary(conflictPath, content);
      }

      // Update sync state for original path to server's revision
      // This marks that we're now aware of the server's version
      this.syncState[originalPath] = { hash, revision };
      await this.saveSyncState();

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
    const file = this.vault.getAbstractFileByPath(path);
    if (!(file instanceof TFile)) {
      throw new Error(`File not found: ${path}`);
    }

    const content = await this.vault.readBinary(file);
    const hashBuffer = await crypto.subtle.digest('SHA-256', content);
    return this.arrayBufferToHex(hashBuffer);
  }

  private async fetchManifest(): Promise<{ files: FileRecord[] }> {
    const response = await fetch(`${this.settings.serverUrl}/manifest`);
    if (!response.ok) {
      throw new Error(`Failed to fetch manifest: ${response.status}`);
    }
    return response.json();
  }

  private async saveSyncState(): Promise<void> {
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
    // Clear all debounce timers
    for (const timer of this.debounceTimers.values()) {
      clearTimeout(timer);
    }
    this.debounceTimers.clear();

    // Unregister event listeners
    for (const ref of this.eventRefs) {
      this.vault.offref(ref);
    }
    this.eventRefs = [];

    console.log('SyncService: Destroyed');
  }
}
