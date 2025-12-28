import { App, Plugin, PluginSettingTab, Setting } from 'obsidian';
import { SyncService } from './sync-service';

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

  async onload() {
    await this.loadSettings();

    console.log('Scion Sync plugin loaded');

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
    console.log('Scion Sync plugin unloaded');
  }

  async loadSettings() {
    const data = (await this.loadData()) as ScionSyncData | null;
    this.settings = Object.assign({}, DEFAULT_SETTINGS, data?.settings);
    this.syncState = data?.syncState || {};
  }

  async saveSettings() {
    await this.saveData({ settings: this.settings, syncState: this.syncState });
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
