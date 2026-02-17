import { Injectable, signal } from '@angular/core';
import { Subject } from 'rxjs';

export interface ConnectionConfig {
  serverUrl: string;
  authToken: string;
  tableName: string;
}

export interface TableData {
  columns: string[];
  rows: any[];
}

export interface TableTransaction {
  add?: any[];
  update?: any[];
  remove?: any[];
}

@Injectable({
  providedIn: 'root'
})
export class DeephavenService {
  private session: any = null;
  private table: any = null;

  readonly isConnected = signal(false);
  readonly isLoading = signal(false);
  readonly error = signal<string | null>(null);
  readonly tableData = signal<TableData | null>(null);

  // Subject for emitting row transactions (add/update/remove)
  readonly transaction$ = new Subject<TableTransaction>();

  // Store row key field for identifying rows
  private rowKeyField: string | null = null;

  // Map to track current rows by key for efficient lookups
  private rowDataMap = new Map<string, any>();

  private client: any = null;

  async connect(config: ConnectionConfig): Promise<void> {
    this.isLoading.set(true);
    this.error.set(null);

    try {
      // Dynamically import DeepHaven API
      const dh = await this.loadDeephavenApi(config.serverUrl);
      console.log('DeepHaven API loaded:', dh);

      // Create client
      this.client = new dh.CoreClient(config.serverUrl);
      console.log('Client created');

      // Login with PSK authentication (doesn't return session)
      await this.client.login({
        type: 'io.deephaven.authentication.psk.PskAuthenticationHandler',
        token: config.authToken
      });
      console.log('Login successful');

      // Get IDE connection - THIS is the session
      this.session = await this.client.getAsIdeConnection();
      console.log('IDE Connection obtained:', this.session);

      if (!this.session) {
        throw new Error('Failed to get IDE connection after login');
      }

      // Get the table
      console.log('Fetching table:', config.tableName);
      this.table = await this.session.getTable(config.tableName);
      console.log('Table fetched:', this.table);

      // Subscribe to table updates
      await this.subscribeToTable();

      this.isConnected.set(true);
    } catch (err: any) {
      console.error('Connection error:', err);
      this.error.set(err.message || 'Failed to connect to DeepHaven');
      throw err;
    } finally {
      this.isLoading.set(false);
    }
  }

  private async loadDeephavenApi(serverUrl: string): Promise<any> {
    // Check if already loaded
    if ((window as any).dh) {
      console.log('DeepHaven API already loaded');
      return (window as any).dh;
    }

    // Use dynamic import as per DeepHaven documentation
    // https://deephaven.io/core/docs/how-to-guides/use-jsapi/
    const apiUrl = `${serverUrl}/jsapi/dh-core.js`;
    console.log(`Loading DeepHaven API from: ${apiUrl}`);

    try {
      const dh = await import(/* webpackIgnore: true */ apiUrl);
      const api = dh.default || dh;
      (window as any).dh = api;
      console.log('DeepHaven API loaded successfully');
      return api;
    } catch (err) {
      console.error('Failed to load DeepHaven API:', err);
      throw new Error(`Failed to load DeepHaven API from ${serverUrl}. Check if the server is accessible and CORS is enabled.`);
    }
  }

  private async subscribeToTable(): Promise<void> {
    if (!this.table) return;

    // Get column names for data extraction
    const columns: string[] = this.table.columns.map((col: any) => col.name);

    // Use the first column as the row key field (typically an ID column)
    this.rowKeyField = columns[0];
    this.rowDataMap.clear();

    // Subscribe to all columns for full table updates
    const subscription = await this.table.subscribe(this.table.columns);

    console.log('Subscribed to table with columns:', columns);

    // Listen for updates - full subscription gives us all rows + deltas
    subscription.addEventListener('updated', (event: any) => {
      this.handleTableUpdate(event, columns);
    });
  }

  private handleTableUpdate(event: any, columns: string[]): void {
    const { rows } = event.detail;
    const isInitialLoad = this.rowDataMap.size === 0;

    // Get column objects for row.get()
    const columnObjects = this.table.columns;

    // Build current rows map
    const currentRows = new Map<string, any>();
    const allRows: any[] = [];

    rows.forEach((row: any, index: number) => {
      const rowData: any = { __rowIndex: index };

      // Extract column values using column objects
      columns.forEach((colName: string) => {
        const colObj = columnObjects.find((c: any) => c.name === colName);
        if (colObj) {
          rowData[colName] = row.get(colObj);
        }
      });

      const rowKey = this.rowKeyField ? String(rowData[this.rowKeyField]) : String(index);
      currentRows.set(rowKey, rowData);
      allRows.push(rowData);
    });

    if (isInitialLoad) {
      // Initial load - set all data
      console.log('Initial load with', allRows.length, 'rows');
      this.rowDataMap = currentRows;
      this.tableData.set({ columns, rows: allRows });
    } else {
      // Detect changes by comparing with previous state
      const addedRows: any[] = [];
      const updatedRows: any[] = [];
      const removedRows: any[] = [];

      // Find added and updated rows
      for (const [key, rowData] of currentRows.entries()) {
        const existingRow = this.rowDataMap.get(key);
        if (!existingRow) {
          // New row
          addedRows.push(rowData);
        } else if (this.hasRowChanged(existingRow, rowData, columns)) {
          // Row was modified
          updatedRows.push(rowData);
        }
      }

      // Find removed rows
      for (const [key, rowData] of this.rowDataMap.entries()) {
        if (!currentRows.has(key)) {
          removedRows.push(rowData);
        }
      }

      // Update our map to current state
      this.rowDataMap = currentRows;

      // Emit transaction if there are changes
      if (addedRows.length > 0 || updatedRows.length > 0 || removedRows.length > 0) {
        const transaction: TableTransaction = {};

        if (addedRows.length > 0) {
          transaction.add = addedRows;
        }
        if (updatedRows.length > 0) {
          transaction.update = updatedRows;
        }
        if (removedRows.length > 0) {
          transaction.remove = removedRows;
        }

        console.log('Emitting transaction:', {
          add: addedRows.length,
          update: updatedRows.length,
          remove: removedRows.length
        });
        this.transaction$.next(transaction);
      }

      // Also update tableData signal for consistency
      this.tableData.set({ columns, rows: allRows });
    }
  }

  private hasRowChanged(oldRow: any, newRow: any, columns: string[]): boolean {
    for (const col of columns) {
      if (oldRow[col] !== newRow[col]) {
        return true;
      }
    }
    return false;
  }

  disconnect(): void {
    if (this.table) {
      this.table.close();
      this.table = null;
    }
    if (this.session) {
      this.session.close();
      this.session = null;
    }
    if (this.client) {
      this.client.disconnect();
      this.client = null;
    }
    this.isConnected.set(false);
    this.tableData.set(null);
    this.rowDataMap.clear();
    this.rowKeyField = null;
  }

  getRowKeyField(): string | null {
    return this.rowKeyField;
  }
}
