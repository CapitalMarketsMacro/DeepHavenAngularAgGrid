import { Component, computed, inject, ViewChild } from '@angular/core';
import { AgGridAngular } from 'ag-grid-angular';
import { ColDef, GridReadyEvent, GridApi, themeQuartz, colorSchemeDarkBlue } from 'ag-grid-community';
import { ConnectionDialogComponent } from './components/connection-dialog/connection-dialog.component';
import { DeephavenService, ConnectionConfig } from './services/deephaven.service';

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [AgGridAngular, ConnectionDialogComponent],
  templateUrl: './app.html',
  styleUrl: './app.scss'
})
export class App {
  @ViewChild(ConnectionDialogComponent) connectionDialog!: ConnectionDialogComponent;

  private readonly deephavenService = inject(DeephavenService);
  private gridApi!: GridApi;

  readonly isConnected = this.deephavenService.isConnected;
  readonly isLoading = this.deephavenService.isLoading;
  readonly error = this.deephavenService.error;
  readonly tableData = this.deephavenService.tableData;

  connectionInfo: ConnectionConfig | null = null;

  readonly columnDefs = computed<ColDef[]>(() => {
    const data = this.tableData();
    if (!data) return [];

    return data.columns.map(col => ({
      field: col,
      headerName: col,
      sortable: true,
      filter: true,
      resizable: true
    }));
  });

  readonly rowData = computed(() => {
    const data = this.tableData();
    return data ? data.rows : [];
  });

  readonly defaultColDef: ColDef = {
    flex: 1,
    minWidth: 100
  };

  readonly theme = themeQuartz.withPart(colorSchemeDarkBlue);

  onGridReady(params: GridReadyEvent): void {
    this.gridApi = params.api;
  }

  async onConnect(config: ConnectionConfig): Promise<void> {
    try {
      await this.deephavenService.connect(config);
      this.connectionInfo = config;
    } catch (err: any) {
      this.connectionDialog.setError(err.message || 'Connection failed');
      this.connectionDialog.setSubmitting(false);
    }
  }

  onDisconnect(): void {
    this.deephavenService.disconnect();
    this.connectionInfo = null;
  }
}
