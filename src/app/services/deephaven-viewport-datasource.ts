import { IViewportDatasource, IViewportDatasourceParams, SortModelItem } from 'ag-grid-community';

/**
 * DeepHaven Viewport Data Source for AG Grid
 *
 * Implements AG Grid's IViewportDatasource interface to efficiently
 * stream data from DeepHaven tables using viewport-based updates.
 * Only rows visible in the grid viewport are fetched and updated.
 *
 * Key API facts:
 * - table.applySort / table.applyFilter are synchronous, mutate in place, return previous value
 * - table.setViewport(first, last, cols) returns a TableViewportSubscription
 * - subscription.setViewport(first, last, cols) updates the range on an existing subscription
 * - column.sort() takes no args, returns a Sort builder with .asc() / .desc()
 * - column.filter() takes no args, returns a FilterValue builder
 */
export class DeephavenViewportDatasource implements IViewportDatasource {
  private params!: IViewportDatasourceParams;
  private table: any;
  private columns: any[];
  private columnMap: Map<string, any>;
  private currentViewport: { firstRow: number; lastRow: number } | null = null;
  private subscription: any = null;
  private cleanupFns: Array<() => void> = [];

  constructor(table: any) {
    this.table = table;
    this.columns = table.columns;
    this.columnMap = new Map();
    for (const col of this.columns) {
      this.columnMap.set(col.name, col);
    }
  }

  /** Access the DeepHaven API from the global namespace */
  private get dh(): any {
    return (window as any).dh;
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
      const column = this.columnMap.get(colId);
      if (!column) {
        console.warn(`Column not found for filtering: ${colId}`);
        continue;
      }

      const condition = this.buildFilterCondition(column, model);
      if (condition) {
        conditions.push(condition);
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
   * Build a single DH FilterCondition from an ag-Grid filter condition
   */
  private buildSingleCondition(column: any, model: any): any {
    const filterType = model.filterType; // 'text', 'number', 'date'
    const type = model.type;             // 'contains', 'equals', 'greaterThan', etc.

    // column.filter() returns a FilterCondition builder (NOT FilterValue)
    const filterValue = column.filter();

    switch (filterType) {
      case 'text':
        return this.buildTextCondition(filterValue, type, model.filter);
      case 'number':
        return this.buildNumberCondition(filterValue, type, model.filter, model.filterTo);
      case 'date':
        return this.buildDateCondition(filterValue, type, model.dateFrom, model.dateTo);
      default:
        console.warn(`Unsupported filter type: ${filterType}`);
        return null;
    }
  }

  /**
   * Build a text filter condition
   * FilterValue.ofString() is a static method on dh.FilterValue, not on the filter builder
   */
  private buildTextCondition(filterValue: any, type: string, value: string): any {
    if (value == null) {
      if (type === 'blank') return filterValue.isNull();
      if (type === 'notBlank') return filterValue.isNull().not();
      return null;
    }

    const target = this.dh.FilterValue.ofString(value);

    switch (type) {
      case 'equals':
        return filterValue.eq(target);
      case 'notEqual':
        return filterValue.notEq(target);
      case 'contains':
        // Use Java String.contains() via invoke to bypass DH $makeContains getLiteral bug
        return filterValue.invoke('contains', this.dh.FilterValue.ofString(value));
      case 'notContains':
        return filterValue.invoke('contains', this.dh.FilterValue.ofString(value)).not();
      case 'startsWith':
        return filterValue.invoke('startsWith', this.dh.FilterValue.ofString(value));
      case 'endsWith':
        return filterValue.invoke('endsWith', this.dh.FilterValue.ofString(value));
      case 'blank':
        return filterValue.isNull();
      case 'notBlank':
        return filterValue.isNull().not();
      default:
        console.warn(`Unsupported text filter type: ${type}`);
        return null;
    }
  }

  /**
   * Build a number filter condition
   * FilterValue.ofNumber() is a static method on dh.FilterValue, not on the filter builder
   */
  private buildNumberCondition(
    filterValue: any,
    type: string,
    value: number,
    valueTo?: number
  ): any {
    if (type === 'blank') return filterValue.isNull();
    if (type === 'notBlank') return filterValue.isNull().not();

    if (value == null) return null;

    const target = this.dh.FilterValue.ofNumber(value);

    switch (type) {
      case 'equals':
        return filterValue.eq(target);
      case 'notEqual':
        return filterValue.notEq(target);
      case 'greaterThan':
        return filterValue.greaterThan(target);
      case 'greaterThanOrEqual':
        return filterValue.greaterThanOrEqualTo(target);
      case 'lessThan':
        return filterValue.lessThan(target);
      case 'lessThanOrEqual':
        return filterValue.lessThanOrEqualTo(target);
      case 'inRange':
        if (valueTo == null) return null;
        const targetTo = this.dh.FilterValue.ofNumber(valueTo);
        return filterValue.greaterThanOrEqualTo(target).and(filterValue.lessThanOrEqualTo(targetTo));
      default:
        console.warn(`Unsupported number filter type: ${type}`);
        return null;
    }
  }

  /**
   * Build a date filter condition
   * Uses dh.DateWrapper.ofJsDate() to convert JS Date, then wraps in FilterValue.ofNumber()
   */
  private buildDateCondition(
    filterValue: any,
    type: string,
    dateFrom?: string,
    dateTo?: string
  ): any {
    if (type === 'blank') return filterValue.isNull();
    if (type === 'notBlank') return filterValue.isNull().not();

    if (dateFrom == null) return null;

    const fromDate = new Date(dateFrom);
    const target = this.dh.FilterValue.ofNumber(this.dh.DateWrapper.ofJsDate(fromDate));

    switch (type) {
      case 'equals':
        return filterValue.eq(target);
      case 'notEqual':
        return filterValue.notEq(target);
      case 'greaterThan':
        return filterValue.greaterThan(target);
      case 'lessThan':
        return filterValue.lessThan(target);
      case 'inRange':
        if (dateTo == null) return null;
        const toDate = new Date(dateTo);
        const targetTo = this.dh.FilterValue.ofNumber(this.dh.DateWrapper.ofJsDate(toDate));
        return filterValue.greaterThanOrEqualTo(target).and(filterValue.lessThanOrEqualTo(targetTo));
      default:
        console.warn(`Unsupported date filter type: ${type}`);
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
