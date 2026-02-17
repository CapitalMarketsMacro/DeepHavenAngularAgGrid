import { Component, computed, inject, ViewChild, OnInit, OnDestroy } from '@angular/core';
import { Subscription } from 'rxjs';
import { AgGridAngular } from 'ag-grid-angular';
import {
  ColDef,
  GridReadyEvent,
  GridApi,
  themeQuartz,
  colorSchemeDarkBlue,
  GetRowIdParams,
  IViewportDatasource,
  SortChangedEvent,
  FilterChangedEvent
} from 'ag-grid-community';
import { ConnectionDialogComponent } from './components/connection-dialog/connection-dialog.component';
import { DeephavenService, ConnectionConfig, TableTransaction } from './services/deephaven.service';
import { DeephavenViewportDatasource } from './services/deephaven-viewport-datasource';

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
  readonly useViewport = this.deephavenService.useViewport;
  readonly viewportDatasource = this.deephavenService.viewportDatasource;

  connectionInfo: ConnectionConfig | null = null;

  // Row model type based on config
  readonly rowModelType = computed(() => {
    return this.useViewport() ? 'viewport' : 'clientSide';
  });

  readonly columnDefs = computed<ColDef[]>(() => {
    const data = this.tableData();
    if (!data) return [];

    // In viewport mode, use type-appropriate filters based on DH column types
    const datasource = this.viewportDatasource();
    const columnTypes = datasource ? datasource.getColumnTypes() : null;

    return data.columns.map(col => {
      const def: ColDef = {
        field: col,
        headerName: col,
        sortable: true,
        filter: true,
        resizable: true
      };

      if (columnTypes) {
        const dhType = columnTypes.get(col);
        def.filter = this.getFilterForType(dhType);
      }

      return def;
    });
  });

  readonly rowData = computed(() => {
    // Only return row data for client-side mode
    if (this.useViewport()) return undefined;
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
    if (keyField && params.data && params.data[keyField] !== undefined) {
      return String(params.data[keyField]);
    }
    return params.data?.__rowIndex !== undefined ? String(params.data.__rowIndex) : String(Math.random());
  };

  ngOnInit(): void {
    // Subscribe to transaction updates for incremental grid updates (client-side mode only)
    this.transactionSubscription = this.deephavenService.transaction$.subscribe(
      (transaction: TableTransaction) => {
        if (!this.useViewport()) {
          this.applyGridTransaction(transaction);
        }
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

    // If using viewport mode, set the viewport datasource
    if (this.useViewport()) {
      const datasource = this.viewportDatasource();
      if (datasource) {
        console.log('Setting viewport datasource on grid');
        this.gridApi.setGridOption('viewportDatasource', datasource as IViewportDatasource);
      }
    }
  }

  /**
   * Handle sort changes - for viewport mode, apply sorting via DeepHaven
   */
  onSortChanged(event: SortChangedEvent): void {
    if (!this.useViewport()) {
      // Client-side mode handles sorting automatically
      return;
    }

    const datasource = this.viewportDatasource();
    if (datasource) {
      const sortModel = this.gridApi.getColumnState()
        .filter(col => col.sort)
        .map(col => ({
          colId: col.colId,
          sort: col.sort as 'asc' | 'desc'
        }));

      console.log('Sort changed in viewport mode:', sortModel);
      (datasource as DeephavenViewportDatasource).applySort(sortModel);
    }
  }

  /**
   * Handle filter changes - for viewport mode, apply filtering via DeepHaven
   */
  onFilterChanged(event: FilterChangedEvent): void {
    if (!this.useViewport()) return;

    const datasource = this.viewportDatasource();
    if (datasource) {
      const filterModel = this.gridApi.getFilterModel();
      console.log('Filter changed in viewport mode:', filterModel);
      (datasource as DeephavenViewportDatasource).applyFilter(filterModel);
    }
  }

  /**
   * Map DeepHaven column type to the appropriate ag-Grid filter component
   */
  private getFilterForType(dhType: string | undefined): string | boolean {
    if (!dhType) return true;

    const t = dhType.toLowerCase();

    if (t.includes('int') || t.includes('long') || t.includes('short') ||
        t.includes('double') || t.includes('float') || t.includes('byte') ||
        t.includes('decimal') || t.includes('bigdecimal')) {
      return 'agNumberColumnFilter';
    }

    if (t.includes('datetime') || t.includes('instant') || t.includes('zoneddatetime')) {
      return 'agDateColumnFilter';
    }

    if (t.includes('string') || t.includes('char')) {
      return 'agTextColumnFilter';
    }

    // boolean and other types: use default
    return true;
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
