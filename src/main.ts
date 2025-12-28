import { App, Modal, Plugin, PluginSettingTab, Setting } from 'obsidian';
import { SyncService, SyncStatus, ScionSyncSettings } from './sync-service';

interface ScionSyncData {
  settings: ScionSyncSettings;
  syncState: Record<string, { hash: string; commit: string }>;
}

const DEFAULT_SETTINGS: ScionSyncSettings = {
  serverUrl: 'http://localhost:3000',
  pollInterval: 30,
  autoSync: true,
  syncOnStartup: true,
  conflictMode: 'merge',
  debounceInterval: 3,
};

export default class ScionSyncPlugin extends Plugin {
  settings: ScionSyncSettings = DEFAULT_SETTINGS;
  private syncService: SyncService | null = null;
  private syncState: Record<string, { hash: string; commit: string }> = {};
  private statusBarItem: HTMLElement | null = null;

  async onload() {
    console.log('ScionSyncPlugin: onload() starting...');
    await this.loadSettings();

    // Get vault name from Obsidian
    const vaultName = this.app.vault.getName();

    console.log('ScionSyncPlugin: Plugin loaded', {
      serverUrl: this.settings.serverUrl,
      vaultName,
      pollInterval: this.settings.pollInterval,
      syncStateEntries: Object.keys(this.syncState).length,
    });

    // Add status bar item
    this.statusBarItem = this.addStatusBarItem();
    this.statusBarItem.addClass('scion-sync-status');
    this.statusBarItem.setText('Scion: Ready');

    // Make status bar clickable to show status modal
    this.statusBarItem.addEventListener('click', () => {
      new SyncStatusModal(this.app, this).open();
    });

    console.log('ScionSyncPlugin: Status bar item added');

    // Initialize sync service with vault name
    this.syncService = new SyncService(
      this.app,
      this.settings,
      vaultName,
      this.syncState,
      async (data) => {
        this.syncState = (data as { syncState: typeof this.syncState }).syncState;
        await this.saveData({ settings: this.settings, syncState: this.syncState });
      }
    );

    // Register status callback
    this.syncService.setStatusCallback((status, message) => {
      this.updateStatusBar(status, message);
    });

    // Register conflict callback
    this.syncService.setConflictCallback((conflict) => {
      if (this.settings.conflictMode === 'ask') {
        new ConflictModal(this.app, this, conflict).open();
      }
    });

    console.log('ScionSyncPlugin: Callbacks registered');

    // Initialize sync on startup (async, don't block plugin load)
    console.log('ScionSyncPlugin: Starting initial sync...');
    this.syncService.initialize();

    // Add settings tab
    this.addSettingTab(new ScionSyncSettingTab(this.app, this));
    console.log('ScionSyncPlugin: Settings tab added');

    // Add ribbon icon
    this.addRibbonIcon('refresh-cw', 'Scion Sync', async () => {
      console.log('ScionSyncPlugin: Manual sync triggered via ribbon icon');
      await this.syncService?.syncAll();
    });
    console.log('ScionSyncPlugin: Ribbon icon added');

    // Add commands
    this.addCommand({
      id: 'sync-now',
      name: 'Sync Now',
      callback: async () => {
        console.log('ScionSyncPlugin: Sync command executed');
        await this.syncService?.syncAll();
      },
    });

    this.addCommand({
      id: 'toggle-auto-sync',
      name: 'Toggle Auto-Sync',
      callback: async () => {
        this.settings.autoSync = !this.settings.autoSync;
        await this.saveSettings();
        this.syncService?.updateSettings(this.settings);
        const status = this.settings.autoSync ? 'enabled' : 'disabled';
        console.log(`ScionSyncPlugin: Auto-sync ${status}`);
      },
    });

    this.addCommand({
      id: 'show-sync-status',
      name: 'Show Sync Status',
      callback: () => {
        new SyncStatusModal(this.app, this).open();
      },
    });

    this.addCommand({
      id: 'resolve-conflicts',
      name: 'Resolve All Conflicts',
      callback: () => {
        const conflicts = this.syncService?.getPendingConflicts() || [];
        if (conflicts.length === 0) {
          console.log('ScionSyncPlugin: No pending conflicts');
          return;
        }
        // Show first conflict
        new ConflictModal(this.app, this, conflicts[0]).open();
      },
    });

    console.log('ScionSyncPlugin: Commands registered');
    console.log('ScionSyncPlugin: onload() complete');
  }

  onunload() {
    console.log('ScionSyncPlugin: onunload() starting...');
    this.syncService?.destroy();
    console.log('ScionSyncPlugin: Plugin unloaded');
  }

  async loadSettings() {
    console.log('ScionSyncPlugin: Loading settings...');
    const data = (await this.loadData()) as ScionSyncData | null;
    this.settings = Object.assign({}, DEFAULT_SETTINGS, data?.settings);
    this.syncState = data?.syncState || {};
    console.log('ScionSyncPlugin: Settings loaded', {
      serverUrl: this.settings.serverUrl,
      pollInterval: this.settings.pollInterval,
      autoSync: this.settings.autoSync,
      syncStateEntries: Object.keys(this.syncState).length,
    });
  }

  async saveSettings() {
    console.log('ScionSyncPlugin: Saving settings...', {
      serverUrl: this.settings.serverUrl,
      pollInterval: this.settings.pollInterval,
      autoSync: this.settings.autoSync,
    });
    await this.saveData({ settings: this.settings, syncState: this.syncState });
    console.log('ScionSyncPlugin: Settings saved');
  }

  getSyncService(): SyncService | null {
    return this.syncService;
  }

  private updateStatusBar(status: SyncStatus, message?: string): void {
    console.log(`ScionSyncPlugin: updateStatusBar called with status: '${status}'`, message ? { message } : '');

    if (!this.statusBarItem) {
      console.warn('ScionSyncPlugin: Status bar item not available');
      return;
    }

    // Remove all status classes
    this.statusBarItem.removeClass('syncing', 'success', 'error');

    // Update content and class based on status
    switch (status) {
      case 'idle':
        this.statusBarItem.setText('Scion: Synced');
        console.log('ScionSyncPlugin: Status bar set to idle');
        break;
      case 'syncing':
        this.statusBarItem.addClass('syncing');
        this.statusBarItem.setText('Scion: Syncing...');
        console.log('ScionSyncPlugin: Status bar set to syncing');
        break;
      case 'success':
        this.statusBarItem.addClass('success');
        this.statusBarItem.setText('Scion: Synced');
        console.log('ScionSyncPlugin: Status bar set to success (will reset in 3s)');
        // Fade back to idle after 3 seconds
        setTimeout(() => {
          this.statusBarItem?.removeClass('success');
          console.log('ScionSyncPlugin: Success state cleared');
        }, 3000);
        break;
      case 'error':
        this.statusBarItem.addClass('error');
        this.statusBarItem.setText(`Scion: Error`);
        this.statusBarItem.setAttr('title', message || 'Unknown error');
        console.log('ScionSyncPlugin: Status bar set to error', { message });
        break;
    }
  }
}

// Settings Tab
class ScionSyncSettingTab extends PluginSettingTab {
  plugin: ScionSyncPlugin;

  constructor(app: App, plugin: ScionSyncPlugin) {
    super(app, plugin);
    this.plugin = plugin;
    console.log('ScionSyncSettingTab: Constructor called');
  }

  display(): void {
    console.log('ScionSyncSettingTab: Displaying settings');
    const { containerEl } = this;

    containerEl.empty();
    containerEl.createEl('h2', { text: 'Scion Sync Settings' });

    // Server URL
    new Setting(containerEl)
      .setName('Server URL')
      .setDesc('The URL of your Scion sync server (e.g., http://192.168.1.100:3000)')
      .addText((text) =>
        text
          .setPlaceholder('http://localhost:3000')
          .setValue(this.plugin.settings.serverUrl)
          .onChange(async (value) => {
            console.log(`ScionSyncSettingTab: Server URL changed to: ${value}`);
            this.plugin.settings.serverUrl = value;
            await this.plugin.saveSettings();
          })
      );

    // Sync interval slider
    new Setting(containerEl)
      .setName('Sync interval')
      .setDesc(`Check for changes every ${this.plugin.settings.pollInterval} seconds`)
      .addSlider((slider) =>
        slider
          .setLimits(5, 120, 5)
          .setValue(this.plugin.settings.pollInterval)
          .setDynamicTooltip()
          .onChange(async (value) => {
            console.log(`ScionSyncSettingTab: Poll interval changed to: ${value}s`);
            this.plugin.settings.pollInterval = value;
            await this.plugin.saveSettings();
            this.plugin.getSyncService()?.updateSettings(this.plugin.settings);
            // Update description
            this.display();
          })
      );

    // Typing debounce slider
    new Setting(containerEl)
      .setName('Typing debounce')
      .setDesc(`Pause sync for ${this.plugin.settings.debounceInterval} seconds after typing stops`)
      .addSlider((slider) =>
        slider
          .setLimits(1, 10, 1)
          .setValue(this.plugin.settings.debounceInterval)
          .setDynamicTooltip()
          .onChange(async (value) => {
            console.log(`ScionSyncSettingTab: Debounce interval changed to: ${value}s`);
            this.plugin.settings.debounceInterval = value;
            await this.plugin.saveSettings();
            this.plugin.getSyncService()?.updateSettings(this.plugin.settings);
            // Update description
            this.display();
          })
      );

    // Auto-sync toggle
    new Setting(containerEl)
      .setName('Auto-sync')
      .setDesc('Automatically sync changes in background')
      .addToggle((toggle) =>
        toggle.setValue(this.plugin.settings.autoSync).onChange(async (value) => {
          console.log(`ScionSyncSettingTab: Auto-sync changed to: ${value}`);
          this.plugin.settings.autoSync = value;
          await this.plugin.saveSettings();
          this.plugin.getSyncService()?.updateSettings(this.plugin.settings);
        })
      );

    // Sync on startup toggle
    new Setting(containerEl)
      .setName('Sync on startup')
      .setDesc('Perform full sync when Obsidian opens')
      .addToggle((toggle) =>
        toggle.setValue(this.plugin.settings.syncOnStartup).onChange(async (value) => {
          console.log(`ScionSyncSettingTab: Sync on startup changed to: ${value}`);
          this.plugin.settings.syncOnStartup = value;
          await this.plugin.saveSettings();
        })
      );

    // Conflict resolution mode
    new Setting(containerEl)
      .setName('Conflict resolution')
      .setDesc('How to handle sync conflicts')
      .addDropdown((dropdown) =>
        dropdown
          .addOption('merge', 'Auto-merge (show markers if needed)')
          .addOption('ask', 'Ask me each time')
          .addOption('local', 'Always keep local version')
          .addOption('remote', 'Always keep server version')
          .setValue(this.plugin.settings.conflictMode)
          .onChange(async (value) => {
            console.log(`ScionSyncSettingTab: Conflict mode changed to: ${value}`);
            this.plugin.settings.conflictMode = value as ScionSyncSettings['conflictMode'];
            await this.plugin.saveSettings();
          })
      );

    // Manual sync button
    containerEl.createEl('h3', { text: 'Actions' });

    new Setting(containerEl)
      .setName('Sync now')
      .setDesc('Manually trigger a full sync')
      .addButton((btn) =>
        btn
          .setButtonText('Sync Now')
          .setCta()
          .onClick(async () => {
            console.log('ScionSyncSettingTab: Manual sync triggered');
            await this.plugin.getSyncService()?.syncAll();
          })
      );

    // Show sync status
    const stats = this.plugin.getSyncService()?.getStats();
    if (stats) {
      containerEl.createEl('h3', { text: 'Status' });

      const statusEl = containerEl.createDiv({ cls: 'scion-sync-status-info' });
      statusEl.createEl('p', { text: `Tracked files: ${stats.trackedFiles}` });
      statusEl.createEl('p', { text: `Last commit: ${stats.lastCommit?.substring(0, 8) || 'None'}` });
      statusEl.createEl('p', { text: `Pending conflicts: ${stats.pendingConflicts}` });
    }
  }
}

// Sync Status Modal
class SyncStatusModal extends Modal {
  plugin: ScionSyncPlugin;

  constructor(app: App, plugin: ScionSyncPlugin) {
    super(app);
    this.plugin = plugin;
  }

  onOpen() {
    const { contentEl } = this;
    contentEl.empty();

    contentEl.createEl('h2', { text: 'Scion Sync Status' });

    const stats = this.plugin.getSyncService()?.getStats();

    // Server info
    const infoEl = contentEl.createDiv({ cls: 'scion-status-info' });
    infoEl.createEl('p', { text: `Server: ${this.plugin.settings.serverUrl}` });
    infoEl.createEl('p', { text: `Vault: ${this.app.vault.getName()}` });
    infoEl.createEl('p', { text: `Auto-sync: ${this.plugin.settings.autoSync ? 'Enabled' : 'Disabled'}` });
    infoEl.createEl('p', { text: `Poll interval: ${this.plugin.settings.pollInterval}s` });

    contentEl.createEl('hr');

    // Sync stats
    if (stats) {
      const statsEl = contentEl.createDiv({ cls: 'scion-status-stats' });
      statsEl.createEl('p', { text: `Files tracked: ${stats.trackedFiles}` });
      statsEl.createEl('p', { text: `Last server commit: ${stats.lastCommit?.substring(0, 8) || 'None'}` });
      statsEl.createEl('p', { text: `Pending conflicts: ${stats.pendingConflicts}` });
    }

    contentEl.createEl('hr');

    // Action buttons
    const actionsEl = contentEl.createDiv({ cls: 'scion-status-actions' });

    new Setting(actionsEl)
      .addButton((btn) =>
        btn
          .setButtonText('Sync Now')
          .setCta()
          .onClick(async () => {
            this.close();
            await this.plugin.getSyncService()?.syncAll();
          })
      )
      .addButton((btn) =>
        btn.setButtonText('Settings').onClick(() => {
          this.close();
          // Open settings tab
          (this.app as unknown as { setting: { open: () => void; openTabById: (id: string) => void } }).setting.open();
          (this.app as unknown as { setting: { openTabById: (id: string) => void } }).setting.openTabById('scion-sync');
        })
      )
      .addButton((btn) =>
        btn.setButtonText('Close').onClick(() => {
          this.close();
        })
      );
  }

  onClose() {
    const { contentEl } = this;
    contentEl.empty();
  }
}

// Conflict Resolution Modal
interface PendingConflict {
  path: string;
  mergedContent: string;
  localContent: string;
  serverCommit: string;
}

class ConflictModal extends Modal {
  plugin: ScionSyncPlugin;
  conflict: PendingConflict;

  constructor(app: App, plugin: ScionSyncPlugin, conflict: PendingConflict) {
    super(app);
    this.plugin = plugin;
    this.conflict = conflict;
  }

  onOpen() {
    const { contentEl } = this;
    contentEl.empty();
    contentEl.addClass('scion-conflict-modal');

    contentEl.createEl('h2', { text: 'Sync Conflict' });
    contentEl.createEl('p', { text: `File: ${this.conflict.path}` });
    contentEl.createEl('p', {
      text: 'The file was modified both locally and on the server.',
      cls: 'scion-conflict-desc',
    });

    // Show merged content preview (with conflict markers)
    contentEl.createEl('h3', { text: 'Merged Content (with conflict markers):' });
    const previewEl = contentEl.createEl('pre', { cls: 'scion-conflict-preview' });
    previewEl.createEl('code', { text: this.conflict.mergedContent.substring(0, 500) + '...' });

    contentEl.createEl('hr');

    // Action buttons
    new Setting(contentEl)
      .setName('Choose resolution')
      .addButton((btn) =>
        btn
          .setButtonText('Keep Local')
          .setWarning()
          .onClick(async () => {
            await this.plugin.getSyncService()?.resolveConflict(this.conflict.path, 'local');
            this.close();
          })
      )
      .addButton((btn) =>
        btn
          .setButtonText('Keep Server')
          .setWarning()
          .onClick(async () => {
            await this.plugin.getSyncService()?.resolveConflict(this.conflict.path, 'remote');
            this.close();
          })
      )
      .addButton((btn) =>
        btn
          .setButtonText('Use Merged (edit markers)')
          .setCta()
          .onClick(async () => {
            // Write merged content and let user edit
            await this.plugin.getSyncService()?.resolveConflict(this.conflict.path, 'merged', this.conflict.mergedContent);
            this.close();
          })
      );
  }

  onClose() {
    const { contentEl } = this;
    contentEl.empty();
  }
}
