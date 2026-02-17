import { Component, computed, inject, ViewChild, OnInit, OnDestroy } from '@angular/core';
import { Subscription } from 'rxjs';
import { AgGridAngular } from 'ag-grid-angular';
import { ColDef, GridReadyEvent, GridApi, themeQuartz, colorSchemeDarkBlue, GetRowIdParams } from 'ag-grid-community';
import { ConnectionDialogComponent } from './components/connection-dialog/connection-dialog.component';
import { DeephavenService, ConnectionConfig, TableTransaction } from './services/deephaven.service';

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [AgGridAngular, ConnectionDialogComponent],
  templateUrl: './app.html',
  styleUrl: './app.scss'
})
export class App implements OnInit, OnDestroy {
  @ViewChild(ConnectionDialogComponent) connectionDialog!: ConnectionDialogComponent;

  private readonly deephavenService = inject(DeephavenService);
  private gridApi!: GridApi;
  private transactionSubscription?: Subscription;

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

  // Row ID function for ag-Grid to identify rows for updates
  getRowId = (params: GetRowIdParams): string => {
    const keyField = this.deephavenService.getRowKeyField();
    if (keyField && params.data[keyField] !== undefined) {
      return String(params.data[keyField]);
    }
    return String(params.data.__rowIndex);
  };

  ngOnInit(): void {
    // Subscribe to transaction updates for incremental grid updates
    this.transactionSubscription = this.deephavenService.transaction$.subscribe(
      (transaction: TableTransaction) => {
        this.applyGridTransaction(transaction);
      }
    );
  }

  ngOnDestroy(): void {
    this.transactionSubscription?.unsubscribe();
  }

  private applyGridTransaction(transaction: TableTransaction): void {
    if (!this.gridApi) {
      console.warn('Grid API not ready, cannot apply transaction');
      return;
    }

    console.log('Applying grid transaction:', transaction);

    this.gridApi.applyTransaction({
      add: transaction.add,
      update: transaction.update,
      remove: transaction.remove
    });
  }

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
