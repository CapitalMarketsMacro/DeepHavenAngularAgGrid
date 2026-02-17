import { Component, output, signal, inject, OnInit } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { HttpClient } from '@angular/common/http';
import { ConnectionConfig, DeephavenService, TableInfo } from '../../services/deephaven.service';

export interface DeephavenConfig {
  serverUrl: string;
  authToken: string;
  isEnterprise?: boolean;
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

  availableTables = signal<TableInfo[]>([]);
  selectedTable = signal('');
  customTableName = signal('');
  useCustomTable = signal(false);

  isSubmitting = signal(false);
  isLoadingConfig = signal(true);
  isLoadingTables = signal(false);
  errorMessage = signal<string | null>(null);
  configError = signal<string | null>(null);

  ngOnInit(): void {
    this.loadConfig();
  }

  private loadConfig(): void {
    this.http.get<DeephavenConfig>('/deephaven-config.json').subscribe({
      next: (config) => {
        this.serverUrl = config.serverUrl;
        this.authToken = config.authToken;
        this.isEnterprise = config.isEnterprise ?? false;
        this.isLoadingConfig.set(false);
        console.log('Loaded DeepHaven config:', {
          serverUrl: config.serverUrl,
          isEnterprise: this.isEnterprise
        });

        // After loading config, fetch available tables
        this.fetchTables();
      },
      error: (err) => {
        console.error('Failed to load DeepHaven config:', err);
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
      isEnterprise: this.isEnterprise
    });
  }

  setSubmitting(value: boolean): void {
    this.isSubmitting.set(value);
  }

  setError(message: string | null): void {
    this.errorMessage.set(message);
  }
}
