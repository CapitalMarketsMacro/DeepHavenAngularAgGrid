import { Injectable, signal } from '@angular/core';

export interface ConnectionConfig {
  serverUrl: string;
  authToken: string;
  tableName: string;
}

export interface TableData {
  columns: string[];
  rows: any[];
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

    const columns = this.table.columns.map((col: any) => col.name);

    // Set viewport to get data
    this.table.setViewport(0, this.table.size > 10000 ? 10000 : this.table.size);

    // Listen for updates
    this.table.addEventListener('updated', (event: any) => {
      this.handleTableUpdate(event, columns);
    });
  }

  private handleTableUpdate(event: any, columns: string[]): void {
    const rows: any[] = [];
    const viewportData = event.detail;

    for (let i = viewportData.offset; i < viewportData.offset + viewportData.rows.length; i++) {
      const row = viewportData.rows[i - viewportData.offset];
      const rowData: any = {};

      columns.forEach((col: string, index: number) => {
        rowData[col] = row.get(this.table.columns[index]);
      });

      rows.push(rowData);
    }

    this.tableData.set({ columns, rows });
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
  }
}
