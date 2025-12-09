import { TableInfo, ColumnInfo, ViewInfo } from './snowflake';

export interface CachedTable {
  qualifiedName: string; // DATABASE.SCHEMA.TABLE
  info: TableInfo;
  columns: ColumnInfo[];
}

export interface CachedColumn {
  qualifiedName: string; // DATABASE.SCHEMA.TABLE.COLUMN
  info: ColumnInfo;
}

export class SchemaCache {
  private tables: Map<string, CachedTable> = new Map();
  private columns: Map<string, CachedColumn> = new Map();
  private views: Map<string, ViewInfo> = new Map();
  private schemas: Set<string> = new Set(); // Store unique schema names

  // Index for partial matching (lowercase for case-insensitive search)
  private tableNameIndex: Map<string, string[]> = new Map();
  private columnNameIndex: Map<string, string[]> = new Map();

  /**
   * Clear all cached data
   */
  clear(): void {
    this.tables.clear();
    this.columns.clear();
    this.views.clear();
    this.schemas.clear();
    this.tableNameIndex.clear();
    this.columnNameIndex.clear();
  }

  /**
   * Load tables into cache
   */
  loadTables(tables: TableInfo[], columns: ColumnInfo[]): void {
    // Group columns by table
    const columnsByTable = new Map<string, ColumnInfo[]>();
    for (const column of columns) {
      const key = this.makeQualifiedName(column.catalog, column.schema, column.tableName);
      if (!columnsByTable.has(key)) {
        columnsByTable.set(key, []);
      }
      columnsByTable.get(key)!.push(column);
    }

    // Cache tables with their columns
    for (const table of tables) {
      const qualifiedName = this.makeQualifiedName(table.catalog, table.schema, table.name);
      const tableColumns = columnsByTable.get(qualifiedName) || [];

      this.tables.set(qualifiedName, {
        qualifiedName,
        info: table,
        columns: tableColumns,
      });

      // Collect unique schema names
      this.schemas.add(table.schema);

      // Index table name for partial matching
      const lowerTableName = table.name.toLowerCase();
      if (!this.tableNameIndex.has(lowerTableName)) {
        this.tableNameIndex.set(lowerTableName, []);
      }
      this.tableNameIndex.get(lowerTableName)!.push(qualifiedName);
    }
  }

  /**
   * Load columns into cache
   */
  loadColumns(columns: ColumnInfo[]): void {
    for (const column of columns) {
      const qualifiedName = this.makeQualifiedName(
        column.catalog,
        column.schema,
        column.tableName,
        column.columnName
      );

      this.columns.set(qualifiedName, {
        qualifiedName,
        info: column,
      });

      // Index column name for partial matching
      const lowerColumnName = column.columnName.toLowerCase();
      if (!this.columnNameIndex.has(lowerColumnName)) {
        this.columnNameIndex.set(lowerColumnName, []);
      }
      this.columnNameIndex.get(lowerColumnName)!.push(qualifiedName);
    }
  }

  /**
   * Load views into cache
   */
  loadViews(views: ViewInfo[]): void {
    for (const view of views) {
      const qualifiedName = this.makeQualifiedName(view.catalog, view.schema, view.name);
      this.views.set(qualifiedName, view);
    }
  }

  /**
   * Get table by qualified name or partial match
   */
  getTable(name: string): CachedTable | undefined {
    // Try exact match first
    const exact = this.tables.get(name);
    if (exact) return exact;

    // Try case-insensitive partial match
    const lowerName = name.toLowerCase();
    const matches = this.tableNameIndex.get(lowerName);
    if (matches && matches.length > 0) {
      return this.tables.get(matches[0]);
    }

    return undefined;
  }

  /**
   * Get column by qualified name
   */
  getColumn(name: string): CachedColumn | undefined {
    return this.columns.get(name);
  }

  /**
   * Get view by qualified name
   */
  getView(name: string): ViewInfo | undefined {
    return this.views.get(name);
  }

  /**
   * Get all columns for a table
   */
  getTableColumns(tableName: string): ColumnInfo[] {
    const table = this.getTable(tableName);
    return table ? table.columns : [];
  }

  /**
   * Search tables by prefix (for autocomplete)
   */
  searchTables(prefix: string, limit: number = 50): CachedTable[] {
    const lowerPrefix = prefix.toLowerCase();
    const results: CachedTable[] = [];

    for (const [qualifiedName, table] of this.tables) {
      if (qualifiedName.toLowerCase().includes(lowerPrefix) ||
          table.info.name.toLowerCase().startsWith(lowerPrefix)) {
        results.push(table);
        if (results.length >= limit) break;
      }
    }

    return results;
  }

  /**
   * Search columns by prefix (for autocomplete)
   */
  searchColumns(prefix: string, tableContext?: string[], limit: number = 50): CachedColumn[] {
    const lowerPrefix = prefix.toLowerCase();
    const results: CachedColumn[] = [];

    // If we have table context, filter columns by those tables
    if (tableContext && tableContext.length > 0) {
      const tableLookup = new Set(tableContext.map(t => t.toLowerCase()));

      for (const [qualifiedName, column] of this.columns) {
        const tableQualifiedName = this.makeQualifiedName(
          column.info.catalog,
          column.info.schema,
          column.info.tableName
        ).toLowerCase();

        if (tableLookup.has(tableQualifiedName) &&
            column.info.columnName.toLowerCase().startsWith(lowerPrefix)) {
          results.push(column);
          if (results.length >= limit) break;
        }
      }
    } else {
      // No table context, search all columns
      for (const [qualifiedName, column] of this.columns) {
        if (column.info.columnName.toLowerCase().startsWith(lowerPrefix)) {
          results.push(column);
          if (results.length >= limit) break;
        }
      }
    }

    return results;
  }

  /**
   * Get all schemas
   */
  getAllSchemas(): string[] {
    return Array.from(this.schemas).sort();
  }

  /**
   * Search schemas by prefix (for autocomplete)
   */
  searchSchemas(prefix: string): string[] {
    const lowerPrefix = prefix.toLowerCase();
    return Array.from(this.schemas)
      .filter(schema => schema.toLowerCase().startsWith(lowerPrefix))
      .sort();
  }

  /**
   * Check if a table exists
   */
  tableExists(name: string): boolean {
    return this.getTable(name) !== undefined;
  }

  /**
   * Check if a column exists in a table
   */
  columnExists(tableName: string, columnName: string): boolean {
    const table = this.getTable(tableName);
    if (!table) return false;

    return table.columns.some(
      col => col.columnName.toLowerCase() === columnName.toLowerCase()
    );
  }

  /**
   * Get cache statistics
   */
  getStats(): { tables: number; columns: number; views: number; schemas: number } {
    return {
      tables: this.tables.size,
      columns: this.columns.size,
      views: this.views.size,
      schemas: this.schemas.size,
    };
  }

  /**
   * Create a qualified name from parts
   */
  private makeQualifiedName(...parts: string[]): string {
    return parts.join('.').toUpperCase();
  }
}
