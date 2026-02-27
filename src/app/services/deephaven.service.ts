import { Injectable, signal } from '@angular/core';
import { Subject } from 'rxjs';
import { DeephavenViewportDatasource } from './deephaven-viewport-datasource';

export interface ConnectionConfig {
  serverUrl: string;
  authToken: string;
  tableName: string;
  isEnterprise?: boolean;
  useViewport?: boolean;
}

export interface TableInfo {
  name: string;
  type: string;
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
  readonly useViewport = signal(false);
  readonly viewportDatasource = signal<DeephavenViewportDatasource | null>(null);
  readonly columnTypes = signal<Map<string, string> | null>(null);

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
    this.useViewport.set(config.useViewport ?? false);

    try {
      // Strip fragment identifiers from URL (breaks WebSocket construction)
      const serverUrl = config.serverUrl.replace(/#.*$/, '').replace(/\/$/, '');

      // Dynamically import DeepHaven API (OSS or Enterprise)
      const dh = await this.loadDeephavenApi(serverUrl, config.isEnterprise);
      console.log('DeepHaven API loaded:', dh);

      // Create client and login
      const ClientClass = dh.CoreClient || dh.Client;
      this.client = new ClientClass(serverUrl);
      console.log('Client created:', ClientClass.name || 'unknown');

      await this.client.login({
        type: 'io.deephaven.authentication.psk.PskAuthenticationHandler',
        token: config.authToken
      });
      console.log('Login successful');

      // Get session: try getAsIdeConnection (Core+), fall back to dh.Ide (Enterprise Iris)
      if (typeof this.client.getAsIdeConnection === 'function') {
        this.session = await this.client.getAsIdeConnection();
        console.log('IDE Connection obtained via getAsIdeConnection');
      } else if (dh.Ide) {
        this.session = new dh.Ide(this.client);
        console.log('IDE session obtained via dh.Ide');
      }

      // Get the table
      if (this.session && typeof this.session.getTable === 'function') {
        // OSS / Core+ path: session has getTable
        console.log('Fetching table via session.getTable:', config.tableName);
        this.table = await this.session.getTable(config.tableName);
      } else {
        // Enterprise Iris path: use getKnownConfigs → designated.getTable()
        console.log('Fetching table via getKnownConfigs:', config.tableName);
        const configs = await this.client.getKnownConfigs();
        console.log('Known configs:', configs);

        // Find matching config by name
        let tableConfig: any = null;
        for (const cfg of configs) {
          if (cfg.name === config.tableName || cfg.designated?.name === config.tableName) {
            tableConfig = cfg;
            break;
          }
        }

        if (!tableConfig) {
          // If no exact match, list available configs for debugging
          const names = configs.map((c: any) => c.name || 'unnamed').join(', ');
          throw new Error(`Table "${config.tableName}" not found in configs. Available: ${names}`);
        }

        console.log('Found table config:', tableConfig);
        this.table = await tableConfig.designated.getTable();
      }
      console.log('Table fetched:', this.table);

      // Build column type map from table columns (works for both modes)
      const types = new Map<string, string>();
      for (const col of this.table.columns) {
        types.set(col.name, col.type);
      }
      this.columnTypes.set(types);

      if (config.useViewport) {
        // Use Viewport Row Model - create datasource for AG Grid
        console.log('Using Viewport Row Model');
        const datasource = new DeephavenViewportDatasource(dh, this.table);
        this.viewportDatasource.set(datasource);

        // Set initial column info for the grid
        const columns = datasource.getColumnNames();
        this.tableData.set({ columns, rows: [] });
      } else {
        // Use Client-Side Row Model - subscribe to full table updates
        console.log('Using Client-Side Row Model');
        await this.subscribeToTable();
      }

      this.isConnected.set(true);
    } catch (err: any) {
      console.error('Connection error:', err);
      this.error.set(err.message || 'Failed to connect to DeepHaven');
      throw err;
    } finally {
      this.isLoading.set(false);
    }
  }

  private async loadDeephavenApi(serverUrl: string, isEnterprise: boolean = false): Promise<any> {
    // Check if already loaded
    if ((window as any).dh) {
      console.log('DeepHaven API already loaded');
      return (window as any).dh;
    }

    try {
      let api: any;

      if (isEnterprise) {
        // Enterprise: load the GWT-compiled Iris API from the server.
        // irisapi.nocache.js is a GWT module that must be loaded as a script tag
        // (not an ES module import). It initializes and exposes the dh API on window.
        api = await this.loadEnterpriseGwtApi(serverUrl);
        console.log('DeepHaven Enterprise API loaded from irisapi');
      } else {
        // OSS: load dh-core.js as an ES module from the server
        const apiUrl = `${serverUrl}/jsapi/dh-core.js`;
        console.log(`Loading DeepHaven OSS API from: ${apiUrl}`);
        const dh = await import(/* webpackIgnore: true */ apiUrl);
        api = dh.default || dh;
        console.log('DeepHaven OSS API loaded successfully');
      }

      (window as any).dh = api;
      return api;
    } catch (err) {
      console.error('Failed to load DeepHaven API:', err);
      const source = isEnterprise ? `${serverUrl}/irisapi/irisapi.nocache.js` : serverUrl;
      throw new Error(`Failed to load DeepHaven API from ${source}. Check if the server is accessible and CORS is enabled.`);
    }
  }

  /**
   * Load the Enterprise DH API by injecting the GWT script tag.
   * irisapi.nocache.js is a GWT-compiled module that initializes asynchronously
   * and exposes the dh API on the window object.
   */
  private loadEnterpriseGwtApi(serverUrl: string): Promise<any> {
    return new Promise((resolve, reject) => {
      const scriptUrl = `${serverUrl}/irisapi/irisapi.nocache.js`;
      console.log(`Loading DeepHaven Enterprise GWT API from: ${scriptUrl}`);

      const script = document.createElement('script');
      script.src = scriptUrl;
      script.async = true;

      const timeout = setTimeout(() => {
        cleanup();
        reject(new Error('Timeout waiting for DH Enterprise API to initialize (15s)'));
      }, 15000);

      // Poll for the dh API to become available on window
      const poll = setInterval(() => {
        if ((window as any).dh) {
          cleanup();
          console.log('DeepHaven Enterprise dh API detected on window');
          resolve((window as any).dh);
        }
      }, 100);

      const cleanup = () => {
        clearTimeout(timeout);
        clearInterval(poll);
      };

      script.onerror = () => {
        cleanup();
        reject(new Error(`Failed to load Enterprise API script: ${scriptUrl}`));
      };

      document.head.appendChild(script);
    });
  }

  async fetchAvailableTables(serverUrl: string, authToken: string, isEnterprise: boolean = false): Promise<TableInfo[]> {
    try {
      // Load the DeepHaven API (OSS or Enterprise)
      const dh = await this.loadDeephavenApi(serverUrl, isEnterprise);

      // Create client and login
      let client: any;
      let session: any;

      if (dh.CoreClient) {
        client = new dh.CoreClient(serverUrl);
        await client.login({
          type: 'io.deephaven.authentication.psk.PskAuthenticationHandler',
          token: authToken
        });
        session = await client.getAsIdeConnection();
      } else {
        const wsUrl = serverUrl.replace(/^http:/, 'ws:').replace(/^https:/, 'wss:').replace(/\/$/, '') + '/socket';
        client = new dh.Client(wsUrl);
        await client.login({ type: 'password', token: authToken });
        session = new dh.Ide(client);
      }

      if (!session) {
        throw new Error('Failed to get IDE connection');
      }

      // Get list of tables from the session
      // The session has a getKnownConfigs or similar method
      const tables: TableInfo[] = [];

      // Try to get table names from session
      // DeepHaven exposes tables via getObject or listObjects
      if (typeof session.getKnownConfigs === 'function') {
        const configs = await session.getKnownConfigs();
        console.log('Known configs:', configs);
      }

      // Use subscribeToFieldUpdates to get available objects
      if (typeof session.subscribeToFieldUpdates === 'function') {
        return new Promise<TableInfo[]>((resolve) => {
          const tableList: TableInfo[] = [];

          session.subscribeToFieldUpdates((updates: any) => {
            console.log('Field updates:', updates);

            if (updates.created) {
              for (const field of updates.created) {
                // Filter for table types
                if (field.type === 'Table' || field.type === 'PartitionedTable' ||
                    field.type?.includes('Table')) {
                  tableList.push({
                    name: field.name,
                    type: field.type
                  });
                }
              }
            }

            // Resolve after getting initial field list
            // Use setTimeout to ensure we capture initial batch
            setTimeout(() => {
              console.log('Available tables:', tableList);
              resolve(tableList);
            }, 500);
          });
        });
      }

      // Fallback: try getConsoleTypes or other methods
      console.log('Session methods:', Object.keys(session));

      return tables;
    } catch (err: any) {
      console.error('Failed to fetch available tables:', err);
      throw new Error(err.message || 'Failed to fetch table list from DeepHaven');
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
    // Clean up viewport datasource if using viewport mode
    const datasource = this.viewportDatasource();
    if (datasource) {
      datasource.destroy();
      this.viewportDatasource.set(null);
    }

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
    this.columnTypes.set(null);
    this.rowDataMap.clear();
    this.rowKeyField = null;
    this.useViewport.set(false);
  }

  getRowKeyField(): string | null {
    return this.rowKeyField;
  }
}
