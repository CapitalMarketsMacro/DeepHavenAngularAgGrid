import { Component, output, signal, inject, OnInit } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { HttpClient } from '@angular/common/http';
import { ConnectionConfig, DeephavenService, TableInfo } from '../../services/deephaven.service';

export interface DeephavenConfig {
  serverUrl: string;
  authToken: string;
  isEnterprise?: boolean;
  useViewport?: boolean;
}

@Component({
  selector: 'app-connection-dialog',
  standalone: true,
  imports: [FormsModule],
  templateUrl: './connection-dialog.component.html',
  styleUrl: './connection-dialog.component.scss'
})
export class ConnectionDialogComponent implements OnInit {
  private readonly http = inject(HttpClient);
  private readonly deephavenService = inject(DeephavenService);

  readonly connect = output<ConnectionConfig>();

  private serverUrl = '';
  private authToken = '';
  private isEnterprise = false;
  private useViewport = false;

  availableTables = signal<TableInfo[]>([]);
  selectedTable = signal('');
  customTableName = signal('');
  useCustomTable = signal(false);

  isSubmitting = signal(false);
  isLoadingConfig = signal(true);
  isLoadingTables = signal(false);
  errorMessage = signal<string | null>(null);
  configError = signal<string | null>(null);

  // Display current mode info
  configMode = signal('');

  ngOnInit(): void {
    this.loadConfig();
  }

  /**
   * Determine which config file to load based on URL parameters
   * URL params: ?enterprise=true|false&viewport=true|false
   * Defaults: enterprise=false, viewport=false
   */
  private getConfigPath(): string {
    const urlParams = new URLSearchParams(window.location.search);

    // Parse URL parameters with defaults
    const enterprise = urlParams.get('enterprise')?.toLowerCase() === 'true';
    const viewport = urlParams.get('viewport')?.toLowerCase() === 'true';

    // Build config filename based on parameters
    const editionPart = enterprise ? 'enterprise' : 'oss';
    const modePart = viewport ? 'viewport' : 'client';

    const configPath = `/deephaven-config-${editionPart}-${modePart}.json`;

    console.log('URL params:', { enterprise, viewport });
    console.log('Loading config from:', configPath);

    // Update display mode
    this.configMode.set(`${enterprise ? 'Enterprise' : 'OSS'} / ${viewport ? 'Viewport' : 'Client-Side'}`);

    return configPath;
  }

  private loadConfig(): void {
    const configPath = this.getConfigPath();

    this.http.get<DeephavenConfig>(configPath).subscribe({
      next: (config) => {
        this.serverUrl = config.serverUrl;
        this.authToken = config.authToken;
        this.isEnterprise = config.isEnterprise ?? false;
        this.useViewport = config.useViewport ?? false;
        this.isLoadingConfig.set(false);
        console.log('Loaded DeepHaven config:', {
          serverUrl: config.serverUrl,
          isEnterprise: this.isEnterprise,
          useViewport: this.useViewport
        });

        // After loading config, fetch available tables
        this.fetchTables();
      },
      error: (err) => {
        console.error('Failed to load DeepHaven config from:', configPath);
        // Fallback to default config
        console.log('Falling back to default config');
        this.loadDefaultConfig();
      }
    });
  }

  private loadDefaultConfig(): void {
    this.http.get<DeephavenConfig>('/deephaven-config.json').subscribe({
      next: (config) => {
        this.serverUrl = config.serverUrl;
        this.authToken = config.authToken;
        this.isEnterprise = config.isEnterprise ?? false;
        this.useViewport = config.useViewport ?? false;
        this.isLoadingConfig.set(false);
        this.configMode.set(`${this.isEnterprise ? 'Enterprise' : 'OSS'} / ${this.useViewport ? 'Viewport' : 'Client-Side'} (default)`);
        console.log('Loaded default DeepHaven config:', {
          serverUrl: config.serverUrl,
          isEnterprise: this.isEnterprise,
          useViewport: this.useViewport
        });

        // After loading config, fetch available tables
        this.fetchTables();
      },
      error: (err) => {
        console.error('Failed to load default DeepHaven config:', err);
        this.configError.set('Failed to load configuration. Please check deephaven-config.json');
        this.isLoadingConfig.set(false);
      }
    });
  }

  private async fetchTables(): Promise<void> {
    this.isLoadingTables.set(true);
    try {
      const tables = await this.deephavenService.fetchAvailableTables(
        this.serverUrl,
        this.authToken,
        this.isEnterprise
      );
      this.availableTables.set(tables);
      console.log('Fetched tables:', tables);
    } catch (err: any) {
      console.error('Failed to fetch tables:', err);
      // Don't show error, just allow manual entry
      this.useCustomTable.set(true);
    } finally {
      this.isLoadingTables.set(false);
    }
  }

  toggleCustomTable(): void {
    this.useCustomTable.set(!this.useCustomTable());
    if (this.useCustomTable()) {
      this.selectedTable.set('');
    } else {
      this.customTableName.set('');
    }
  }

  getTableName(): string {
    return this.useCustomTable() ? this.customTableName() : this.selectedTable();
  }

  onSubmit(): void {
    const tableName = this.getTableName();

    if (!tableName) {
      this.errorMessage.set('Please select or enter a table name');
      return;
    }

    if (!this.serverUrl || !this.authToken) {
      this.errorMessage.set('Configuration not loaded. Please refresh the page.');
      return;
    }

    this.errorMessage.set(null);
    this.isSubmitting.set(true);

    this.connect.emit({
      serverUrl: this.serverUrl,
      authToken: this.authToken,
      tableName: tableName,
      isEnterprise: this.isEnterprise,
      useViewport: this.useViewport
    });
  }

  setSubmitting(value: boolean): void {
    this.isSubmitting.set(value);
  }

  setError(message: string | null): void {
    this.errorMessage.set(message);
  }
}
