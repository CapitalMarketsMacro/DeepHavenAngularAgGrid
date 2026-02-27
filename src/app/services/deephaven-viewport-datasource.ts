import { IViewportDatasource, IViewportDatasourceParams, SortModelItem } from 'ag-grid-community';

/**
 * DeepHaven Viewport Data Source for AG Grid
 *
 * Implements AG Grid's IViewportDatasource interface to efficiently
 * stream data from DeepHaven tables using viewport-based updates.
 * Only rows visible in the grid viewport are fetched and updated.
 *
 * Filter implementation follows the official DeepHaven AG Grid plugin:
 * https://github.com/deephaven/deephaven-plugins/blob/main/plugins/ag-grid/src/js/src/utils/AgGridFilterUtils.ts
 */
export class DeephavenViewportDatasource implements IViewportDatasource {
  private params!: IViewportDatasourceParams;
  private table: any;
  private dh: any;
  private columns: any[];
  private columnMap: Map<string, any>;
  private currentViewport: { firstRow: number; lastRow: number } | null = null;
  private subscription: any = null;
  private cleanupFns: Array<() => void> = [];

  /**
   * Resolved FilterValue factory function.
   * OSS: uses dh.FilterValue.ofString/ofNumber (protobuf-compatible).
   * Enterprise: auto-detected from column.filter().constructor or dh.FilterValue.
   */
  private _makeStringValue: ((v: string) => any) | null = null;
  private _makeNumberValue: ((v: number | any) => any) | null = null;

  constructor(dh: any, table: any) {
    this.dh = dh;
    this.table = table;
    this.columns = table.columns;
    this.columnMap = new Map();
    for (const col of this.columns) {
      this.columnMap.set(col.name, col);
    }
  }

  /**
   * One-time detection of how to create filter values.
   * Tries multiple approaches in order until one produces a value
   * compatible with column.filter().eq().
   */
  private detectFilterValueFactory(column: any): void {
    if (this._makeStringValue) return; // already resolved

    // Log diagnostic info for debugging
    console.log('=== FilterValue Detection ===');

    // Approach 1: Standard dh.FilterValue (works on OSS)
    if (this.dh.FilterValue?.ofString) {
      try {
        const test = this.dh.FilterValue.ofString('_test_');
        console.log('dh.FilterValue.ofString result:', typeof test,
          'toArray:', typeof test?.toArray,
          'descriptor:', typeof test?.descriptor,
          'keys:', test ? Object.keys(test) : 'null');

        if (typeof test?.toArray === 'function') {
          this._makeStringValue = (v: string) => this.dh.FilterValue.ofString(v);
          this._makeNumberValue = (v: any) => this.dh.FilterValue.ofNumber(v);
          console.log('FilterValue: using dh.FilterValue (OSS/standard)');
          return;
        }
      } catch (e) {
        console.log('dh.FilterValue.ofString threw:', e);
      }
    }

    // Approach 2: Derive from column.filter().constructor (Core+ protobuf class)
    try {
      const colFilter = column.filter();
      const ctor = colFilter?.constructor;
      console.log('column.filter() result:', typeof colFilter,
        'constructor:', ctor?.name,
        'ctor keys:', ctor ? Object.getOwnPropertyNames(ctor).join(',') : 'none');
      console.log('column.filter() keys:', colFilter ? Object.keys(colFilter) : 'none');
      console.log('column.filter() proto keys:', colFilter
        ? Object.getOwnPropertyNames(Object.getPrototypeOf(colFilter)).join(',') : 'none');

      if (typeof ctor?.ofString === 'function') {
        const test = ctor.ofString('_test_');
        console.log('constructor.ofString result:', typeof test,
          'toArray:', typeof test?.toArray,
          'descriptor:', typeof test?.descriptor);
        this._makeStringValue = (v: string) => ctor.ofString(v);
        this._makeNumberValue = (v: any) => ctor.ofNumber(v);
        console.log('FilterValue: using column.filter().constructor');
        return;
      }
    } catch (e) {
      console.log('column.filter().constructor approach failed:', e);
    }

    // Approach 3: Try dh.FilterValue but pass through (Enterprise Iris API style)
    // Enterprise Iris docs show: column.filter().eqIgnoreCase("FOO")
    // The Iris API may accept raw values OR dh.FilterValue descriptors
    console.log('Approach 3: will use dh.FilterValue.ofString (Enterprise descriptor)');
    if (this.dh.FilterValue?.ofString) {
      this._makeStringValue = (v: string) => this.dh.FilterValue.ofString(v);
      this._makeNumberValue = (v: any) => this.dh.FilterValue.ofNumber(v);
      console.log('FilterValue: using dh.FilterValue (Enterprise descriptor)');
      return;
    }

    // Approach 4: Raw values as last resort
    console.warn('No FilterValue factory found, using raw values');
    this._makeStringValue = (v: string) => v;
    this._makeNumberValue = (v: any) => v;

    // Dump full diagnostic info
    console.log('dh keys:', Object.keys(this.dh));
    console.log('dh.FilterValue:', this.dh.FilterValue);
    console.log('dh.FilterCondition:', this.dh.FilterCondition,
      'keys:', this.dh.FilterCondition ? Object.keys(this.dh.FilterCondition) : 'none');
  }

  /** Create a string filter value using the detected factory */
  private ofString(value: string): any {
    return this._makeStringValue!(value);
  }

  /** Create a number filter value using the detected factory */
  private ofNumber(value: number | any): any {
    return this._makeNumberValue!(value);
  }

  /**
   * Called by AG Grid to initialize the datasource
   */
  init(params: IViewportDatasourceParams): void {
    this.params = params;
    this.params.setRowCount(this.table.size);

    // Listen for table size changes
    const removeSizeListener = this.table.addEventListener('sizechanged', (event: any) => {
      const newSize = event.detail;
      this.params.setRowCount(newSize);
      console.log('Table size changed:', newSize);
    });
    this.cleanupFns.push(removeSizeListener);

    console.log('DeepHaven Viewport Datasource initialized with', this.table.size, 'rows');
  }

  /**
   * Called by AG Grid when the viewport changes (user scrolls)
   */
  setViewportRange(firstRow: number, lastRow: number): void {
    if (firstRow < 0 || lastRow < 0 || lastRow < firstRow) {
      console.log(`Skipping invalid viewport range: ${firstRow} - ${lastRow}`);
      return;
    }

    console.log(`Viewport range requested: ${firstRow} - ${lastRow}`);
    this.currentViewport = { firstRow, lastRow };

    if (this.subscription) {
      // Update range on existing subscription — no teardown needed
      this.subscription.setViewport(firstRow, lastRow, this.columns);
    } else {
      // First time: create the subscription
      this.createSubscription(firstRow, lastRow);
    }
  }

  /**
   * Create a new viewport subscription on the table.
   * Called once on first setViewportRange, and again after sort/filter changes.
   */
  private createSubscription(firstRow: number, lastRow: number): void {
    // Close any existing subscription
    if (this.subscription) {
      this.subscription.close();
      this.subscription = null;
    }

    try {
      // table.setViewport returns a TableViewportSubscription
      this.subscription = this.table.setViewport(firstRow, lastRow, this.columns);

      const removeUpdatedListener = this.subscription.addEventListener(
        'updated',
        (event: any) => this.handleViewportUpdate(event)
      );
      this.cleanupFns.push(removeUpdatedListener);

      console.log(`Viewport subscription created: ${firstRow} - ${lastRow}`);
    } catch (err) {
      console.error('Failed to create viewport subscription:', err);
    }
  }

  /**
   * Re-create subscription from row 0 after a sort/filter change.
   * Uses the previous viewport size for the range, clamped to the new table size.
   * ag-Grid will call setViewportRange() shortly after to adjust the range.
   */
  private resubscribeFromStart(): void {
    const size = this.table.size;
    if (size === 0) {
      this.currentViewport = null;
      return;
    }

    const prevSize = this.currentViewport
      ? this.currentViewport.lastRow - this.currentViewport.firstRow
      : 50;
    const lastRow = Math.min(prevSize, size - 1);
    this.currentViewport = { firstRow: 0, lastRow };
    this.createSubscription(0, lastRow);
  }

  /**
   * Apply sorting to the DeepHaven table (synchronous, mutates in place)
   */
  applySort(sortModel: SortModelItem[]): void {
    console.log('Applying sort:', sortModel);

    const sorts: any[] = [];

    for (const item of sortModel) {
      const column = this.columnMap.get(item.colId);
      if (!column) {
        console.warn(`Column not found for sorting: ${item.colId}`);
        continue;
      }
      if (typeof column.sort !== 'function') {
        console.warn(`Column ${item.colId} does not have sort method`);
        continue;
      }
      // column.sort() takes no args → returns Sort builder with .asc() / .desc()
      const sortBuilder = column.sort();
      sorts.push(item.sort === 'asc' ? sortBuilder.asc() : sortBuilder.desc());
    }

    // applySort is synchronous, mutates the table in place
    this.table.applySort(sorts);
    console.log('Sort applied:', sorts.length, 'columns');

    // Close old subscription
    if (this.subscription) {
      this.subscription.close();
      this.subscription = null;
    }

    // Update row count
    this.params.setRowCount(this.table.size);

    // Re-create subscription starting at row 0 with a safe range
    this.resubscribeFromStart();
  }

  /**
   * Apply filtering to the DeepHaven table (synchronous, mutates in place)
   * Converts ag-Grid filter model to DH FilterCondition[] and calls table.applyFilter()
   */
  applyFilter(filterModel: Record<string, any>): void {
    console.log('Applying filter:', filterModel);

    const conditions: any[] = [];

    for (const [colId, model] of Object.entries(filterModel)) {
      // Use table.findColumn() to match the official DH plugin
      const column = this.table.findColumn(colId);
      if (!column) {
        console.warn(`Column not found for filtering: ${colId}`);
        continue;
      }

      // One-time: detect how to create filter values for this API
      this.detectFilterValueFactory(column);

      try {
        const condition = this.buildFilterCondition(column, model);
        if (condition) {
          conditions.push(condition);
        }
      } catch (err) {
        console.error(`Failed to build filter for column ${colId}:`, err);
      }
    }

    // applyFilter is synchronous, mutates the table in place
    this.table.applyFilter(conditions);
    console.log('Filter applied:', conditions.length, 'conditions');

    // Close old subscription
    if (this.subscription) {
      this.subscription.close();
      this.subscription = null;
    }

    // Update row count
    this.params.setRowCount(this.table.size);

    // Re-create subscription starting at row 0 with a safe range
    this.resubscribeFromStart();
  }

  /**
   * Convert a single ag-Grid column filter model to a DH FilterCondition
   */
  private buildFilterCondition(column: any, model: any): any {
    // ag-Grid combined filter (operator: 'AND' | 'OR' with conditions array)
    if (model.operator) {
      const subConditions = (model.conditions as any[])
        .map((cond: any) => this.buildSingleCondition(column, cond))
        .filter(Boolean);

      if (subConditions.length === 0) return null;
      if (subConditions.length === 1) return subConditions[0];

      // Combine with AND or OR
      let combined = subConditions[0];
      for (let i = 1; i < subConditions.length; i++) {
        combined = model.operator === 'OR'
          ? combined.or(subConditions[i])
          : combined.and(subConditions[i]);
      }
      return combined;
    }

    // Single condition
    return this.buildSingleCondition(column, model);
  }

  /**
   * Build a single DH FilterCondition from an ag-Grid filter condition.
   * Passes column (not column.filter()) to each builder so they can call
   * column.filter() fresh for each operation, matching the reference implementation.
   */
  private buildSingleCondition(column: any, model: any): any {
    switch (model.filterType) {
      case 'text':
        return this.buildTextCondition(column, model);
      case 'number':
        return this.buildNumberCondition(column, model);
      case 'date':
        return this.buildDateCondition(column, model);
      default:
        console.warn(`Unsupported filter type: ${model.filterType}`);
        return null;
    }
  }

  /**
   * Build a text filter condition.
   * Matches the official DH AG Grid plugin: uses dh.FilterValue.ofString() (static),
   * contains() (not containsIgnoreCase), and invoke() for startsWith/endsWith.
   */
  private buildTextCondition(column: any, model: any): any {
    const filterValue = this.ofString(model.filter ?? '');

    switch (model.type) {
      case 'equals':
        return column.filter().eq(filterValue);
      case 'notEqual':
        return column.filter().notEq(filterValue);
      case 'contains':
        return column.filter().contains(filterValue);
      case 'notContains':
        return column.filter().isNull()
          .or(column.filter().contains(filterValue).not());
      case 'startsWith':
        return column.filter().isNull().not()
          .and(column.filter().invoke('startsWith', filterValue));
      case 'endsWith':
        return column.filter().isNull().not()
          .and(column.filter().invoke('endsWith', filterValue));
      case 'blank':
        return column.filter().isNull()
          .or(column.filter().eq(filterValue));
      case 'notBlank':
        return column.filter().isNull().not()
          .and(column.filter().notEq(filterValue));
      default:
        console.warn(`Unsupported text filter type: ${model.type}`);
        return null;
    }
  }

  /**
   * Build a number filter condition.
   * Matches the official DH AG Grid plugin.
   */
  private buildNumberCondition(column: any, model: any): any {
    switch (model.type) {
      case 'blank':
        return column.filter().isNull();
      case 'notBlank':
        return column.filter().isNull().not();
    }

    if (model.filter == null) return null;

    const filterValue = this.ofNumber(model.filter);

    switch (model.type) {
      case 'equals':
        return column.filter().eq(filterValue);
      case 'notEqual':
        return column.filter().notEq(filterValue);
      case 'greaterThan':
        return column.filter().greaterThan(filterValue);
      case 'greaterThanOrEqual':
        return column.filter().greaterThanOrEqualTo(filterValue);
      case 'lessThan':
        return column.filter().lessThan(filterValue);
      case 'lessThanOrEqual':
        return column.filter().lessThanOrEqualTo(filterValue);
      case 'inRange': {
        if (model.filterTo == null) return null;
        const filterValueTo = this.ofNumber(model.filterTo);
        return column.filter().greaterThan(filterValue)
          .and(column.filter().lessThan(filterValueTo));
      }
      default:
        console.warn(`Unsupported number filter type: ${model.type}`);
        return null;
    }
  }

  /**
   * Build a date filter condition.
   * Matches the official DH AG Grid plugin.
   */
  private buildDateCondition(column: any, model: any): any {
    switch (model.type) {
      case 'blank':
        return column.filter().isNull();
      case 'notBlank':
        return column.filter().isNull().not();
    }

    if (model.dateFrom == null) return null;

    const filterValue = this.ofNumber(
      this.dh.DateWrapper.ofJsDate(new Date(model.dateFrom))
    );

    switch (model.type) {
      case 'equals':
        return column.filter().eq(filterValue);
      case 'notEqual':
        return column.filter().notEq(filterValue);
      case 'greaterThan':
        return column.filter().greaterThan(filterValue);
      case 'lessThan':
        return column.filter().lessThan(filterValue);
      case 'inRange': {
        if (model.dateTo == null) return null;
        const filterValueTo = this.ofNumber(
          this.dh.DateWrapper.ofJsDate(new Date(model.dateTo))
        );
        return column.filter().greaterThan(filterValue)
          .and(column.filter().lessThan(filterValueTo));
      }
      default:
        console.warn(`Unsupported date filter type: ${model.type}`);
        return null;
    }
  }

  /**
   * Handle viewport data updates from DeepHaven
   */
  private handleViewportUpdate(event: any): void {
    const { rows, offset } = event.detail;

    if (!rows) return;

    const rowData: { [key: number]: any } = {};

    rows.forEach((row: any, index: number) => {
      const rowIndex = offset + index;
      const data: any = {};

      // Extract column values using the pre-built column map
      for (const [colName, colObj] of this.columnMap) {
        data[colName] = row.get(colObj);
      }

      rowData[rowIndex] = data;
    });

    this.params.setRowData(rowData);
    console.log(`Viewport updated: ${Object.keys(rowData).length} rows from offset ${offset}`);
  }

  /**
   * Called by AG Grid when the datasource is no longer needed
   */
  destroy(): void {
    // Run all cleanup functions (removeEventListener calls)
    for (const fn of this.cleanupFns) {
      try { fn(); } catch (_) { /* ignore */ }
    }
    this.cleanupFns = [];

    if (this.subscription) {
      this.subscription.close();
      this.subscription = null;
    }
    console.log('DeepHaven Viewport Datasource destroyed');
  }

  /**
   * Get column names
   */
  getColumnNames(): string[] {
    return Array.from(this.columnMap.keys());
  }

  /**
   * Get column name → DH type map for type-aware ag-Grid filters
   */
  getColumnTypes(): Map<string, string> {
    const types = new Map<string, string>();
    for (const col of this.columns) {
      types.set(col.name, col.type);
    }
    return types;
  }
}
