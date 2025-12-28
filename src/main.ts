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
    await this.loadSettings();

    console.log('Scion Sync scionsync loaded');

    // Add status bar item
    this.statusBarItem = this.addStatusBarItem();
    this.statusBarItem.addClass('scion-sync-status');
    this.updateStatusBar('idle');

    // Initialize sync service
    this.syncService = new SyncService(
      this.app,
      this.settings,
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

    // Initialize sync on startup (async, don't block plugin load)
    this.syncService.initialize();

    // Add settings tab
    this.addSettingTab(new ScionSyncSettingTab(this.app, this));

    // Add ribbon icon
    this.addRibbonIcon('refresh-cw', 'Scion Sync', async () => {
      console.log('Manual sync triggered');
      await this.syncService?.syncAll();
    });

    // Add command
    this.addCommand({
      id: 'sync-now',
      name: 'Sync Now',
      callback: async () => {
        console.log('Sync command executed');
        await this.syncService?.syncAll();
      },
    });
  }

  onunload() {
    this.syncService?.destroy();
    console.log('Scion Sync scionsync unloaded');
  }

  async loadSettings() {
    const data = (await this.loadData()) as ScionSyncData | null;
    this.settings = Object.assign({}, DEFAULT_SETTINGS, data?.settings);
    this.syncState = data?.syncState || {};
  }

  async saveSettings() {
    await this.saveData({ settings: this.settings, syncState: this.syncState });
  }

  private updateStatusBar(status: SyncStatus, message?: string): void {
    if (!this.statusBarItem) return;

    // Remove all status classes
    this.statusBarItem.removeClass('syncing', 'success', 'error');

    // Update content and class based on status
    switch (status) {
      case 'idle':
        this.statusBarItem.setText('Scion: Synced');
        break;
      case 'syncing':
        this.statusBarItem.addClass('syncing');
        this.statusBarItem.setText('Scion: Syncing...');
        break;
      case 'success':
        this.statusBarItem.addClass('success');
        this.statusBarItem.setText('Scion: Synced');
        // Fade back to idle after 3 seconds
        setTimeout(() => {
          this.statusBarItem?.removeClass('success');
        }, 3000);
        break;
      case 'error':
        this.statusBarItem.addClass('error');
        this.statusBarItem.setText(`Scion: Sync failed`);
        this.statusBarItem.setAttr('title', message || 'Unknown error');
        break;
    }
  }
}

class ScionSyncSettingTab extends PluginSettingTab {
  plugin: ScionSyncPlugin;

  constructor(app: App, plugin: ScionSyncPlugin) {
    super(app, plugin);
    this.plugin = plugin;
  }

  display(): void {
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
            this.plugin.settings.serverUrl = value;
            await this.plugin.saveSettings();
          })
      );
  }
}
