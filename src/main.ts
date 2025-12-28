import { App, Plugin, PluginSettingTab, Setting } from 'obsidian';
import { SyncService, SyncStatus } from './sync-service';

interface ScionSyncSettings {
  serverUrl: string;
}

interface ScionSyncData {
  settings: ScionSyncSettings;
  syncState: Record<string, { hash: string; revision: number }>;
}

const DEFAULT_SETTINGS: ScionSyncSettings = {
  serverUrl: 'http://localhost:3000',
};

export default class ScionSyncPlugin extends Plugin {
  settings: ScionSyncSettings = DEFAULT_SETTINGS;
  private syncService: SyncService | null = null;
  private syncState: Record<string, { hash: string; revision: number }> = {};
  private statusBarItem: HTMLElement | null = null;

  async onload() {
    console.log('ScionSyncPlugin: onload() starting...');
    await this.loadSettings();

    // Get vault name from Obsidian
    const vaultName = this.app.vault.getName();

    console.log('ScionSyncPlugin: Plugin loaded', {
      serverUrl: this.settings.serverUrl,
      vaultName,
      syncStateEntries: Object.keys(this.syncState).length,
    });

    // Add status bar item
    this.statusBarItem = this.addStatusBarItem();
    this.statusBarItem.addClass('scion-sync-status');
    this.updateStatusBar('idle');
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
    console.log('ScionSyncPlugin: Status callback registered');

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

    // Add command
    this.addCommand({
      id: 'sync-now',
      name: 'Sync Now',
      callback: async () => {
        console.log('ScionSyncPlugin: Sync command executed');
        await this.syncService?.syncAll();
      },
    });
    console.log('ScionSyncPlugin: Command registered');

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
      syncStateEntries: Object.keys(this.syncState).length,
    });
  }

  async saveSettings() {
    console.log('ScionSyncPlugin: Saving settings...', {
      serverUrl: this.settings.serverUrl,
      syncStateEntries: Object.keys(this.syncState).length,
    });
    await this.saveData({ settings: this.settings, syncState: this.syncState });
    console.log('ScionSyncPlugin: Settings saved');
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
        this.statusBarItem.setText(`Scion: Sync failed`);
        this.statusBarItem.setAttr('title', message || 'Unknown error');
        console.log('ScionSyncPlugin: Status bar set to error', { message });
        break;
    }
  }
}

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
  }
}
