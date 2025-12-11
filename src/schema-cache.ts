import { TableInfo, ColumnInfo, ViewInfo, WarehouseInfo, RoleInfo, UserInfo, DatabaseInfo } from './snowflake';

export interface CachedTable {
  qualifiedName: string; // DATABASE.SCHEMA.TABLE
  info: TableInfo;
  columns: ColumnInfo[];
}

export interface CachedColumn {
  qualifiedName: string; // DATABASE.SCHEMA.TABLE.COLUMN
  info: ColumnInfo;
}

export interface DDLCache {
  ddl: string;
  fetchedAt: number; // timestamp
}

export class SchemaCache {
  private tables: Map<string, CachedTable> = new Map();
  private columns: Map<string, CachedColumn> = new Map();
  private views: Map<string, ViewInfo> = new Map();
  private schemas: Set<string> = new Set(); // Store unique schema names
  private ddlCache: Map<string, DDLCache> = new Map(); // DDL cache with TTL
  private tablesWithColumns: Set<string> = new Set(); // Track which tables have columns loaded

  // Advanced objects
  private warehouses: Map<string, WarehouseInfo> = new Map(); // Warehouse name -> info
  private roles: Map<string, RoleInfo> = new Map(); // Role name -> info
  private users: Map<string, UserInfo> = new Map(); // User name -> info
  private databases: Map<string, DatabaseInfo> = new Map(); // Database name -> info

  // Index for partial matching (lowercase for case-insensitive search)
  private tableNameIndex: Map<string, string[]> = new Map();
  private columnNameIndex: Map<string, string[]> = new Map();

  // DDL cache TTL (24 hours in milliseconds)
  private readonly DDL_CACHE_TTL = 24 * 60 * 60 * 1000;

  /**
   * Clear all cached data
   */
  clear(): void {
    this.tables.clear();
    this.columns.clear();
    this.views.clear();
    this.schemas.clear();
    this.ddlCache.clear();
    this.tablesWithColumns.clear();
    this.tableNameIndex.clear();
    this.columnNameIndex.clear();
    this.warehouses.clear();
    this.roles.clear();
    this.users.clear();
    this.databases.clear();
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
    const upperName = name.toUpperCase();

    // Try exact match first
    const exact = this.tables.get(upperName);
    if (exact) return exact;

    // Try partial match for schema.table format
    // Check if any full qualified name ends with the provided name
    for (const [qualifiedName, table] of this.tables) {
      // Match SCHEMA.TABLE or DATABASE.SCHEMA.TABLE
      if (qualifiedName.endsWith('.' + upperName) ||
          qualifiedName === upperName) {
        return table;
      }
    }

    // Try case-insensitive table name only
    const lowerName = name.split('.').pop()?.toLowerCase() || name.toLowerCase();
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
   * Check if DDL is cached and not expired
   */
  hasDDL(qualifiedName: string): boolean {
    const cached = this.ddlCache.get(qualifiedName.toUpperCase());
    if (!cached) return false;

    const now = Date.now();
    const isExpired = now - cached.fetchedAt > this.DDL_CACHE_TTL;

    if (isExpired) {
      this.ddlCache.delete(qualifiedName.toUpperCase());
      return false;
    }

    return true;
  }

  /**
   * Get cached DDL
   */
  getDDL(qualifiedName: string): string | undefined {
    if (!this.hasDDL(qualifiedName)) return undefined;

    const cached = this.ddlCache.get(qualifiedName.toUpperCase());
    return cached?.ddl;
  }

  /**
   * Cache DDL with current timestamp
   */
  cacheDDL(qualifiedName: string, ddl: string): void {
    this.ddlCache.set(qualifiedName.toUpperCase(), {
      ddl,
      fetchedAt: Date.now(),
    });
  }

  /**
   * Check if table has columns loaded
   */
  hasColumns(qualifiedName: string): boolean {
    return this.tablesWithColumns.has(qualifiedName.toUpperCase());
  }

  /**
   * Ensure columns are loaded for a table (lazy loading)
   * @param qualifiedName - DATABASE.SCHEMA.TABLE
   * @param fetcher - Function to fetch columns if not cached
   */
  async ensureColumnsLoaded(
    qualifiedName: string,
    fetcher: (db: string, schema: string, table: string) => Promise<ColumnInfo[]>
  ): Promise<void> {
    const upperName = qualifiedName.toUpperCase();

    // Already loaded?
    if (this.tablesWithColumns.has(upperName)) {
      return;
    }

    // Parse qualified name
    const parts = upperName.split('.');
    if (parts.length !== 3) {
      throw new Error(`Invalid qualified name: ${qualifiedName}`);
    }

    const [db, schema, table] = parts;

    // Fetch columns
    try {
      const columns = await fetcher(db, schema, table);

      // Add to cache
      for (const column of columns) {
        const colQualifiedName = this.makeQualifiedName(
          column.catalog,
          column.schema,
          column.tableName,
          column.columnName
        );

        this.columns.set(colQualifiedName, {
          qualifiedName: colQualifiedName,
          info: column,
        });

        // Index column name
        const lowerColumnName = column.columnName.toLowerCase();
        if (!this.columnNameIndex.has(lowerColumnName)) {
          this.columnNameIndex.set(lowerColumnName, []);
        }
        this.columnNameIndex.get(lowerColumnName)!.push(colQualifiedName);
      }

      // Update table's columns array
      const table_obj = this.tables.get(upperName);
      if (table_obj) {
        table_obj.columns = columns;
      }

      // Mark as loaded
      this.tablesWithColumns.add(upperName);
    } catch (error) {
      console.error(`Failed to load columns for ${qualifiedName}:`, error);
      throw error;
    }
  }

  /**
   * Create a qualified name from parts
   */
  private makeQualifiedName(...parts: string[]): string {
    return parts.join('.').toUpperCase();
  }

  /**
   * Load warehouses into cache
   */
  loadWarehouses(warehouses: WarehouseInfo[]): void {
    this.warehouses.clear();
    for (const warehouse of warehouses) {
      this.warehouses.set(warehouse.name.toUpperCase(), warehouse);
    }
    console.log(`Loaded ${warehouses.length} warehouses into cache`);
  }

  /**
   * Get all warehouses
   */
  getWarehouses(): WarehouseInfo[] {
    return Array.from(this.warehouses.values());
  }

  /**
   * Search warehouses by prefix
   */
  searchWarehouses(prefix: string): WarehouseInfo[] {
    const lowerPrefix = prefix.toLowerCase();
    return this.getWarehouses().filter(wh =>
      wh.name.toLowerCase().startsWith(lowerPrefix)
    );
  }

  /**
   * Load roles into cache
   */
  loadRoles(roles: RoleInfo[]): void {
    this.roles.clear();
    for (const role of roles) {
      this.roles.set(role.name.toUpperCase(), role);
    }
    console.log(`Loaded ${roles.length} roles into cache`);
  }

  /**
   * Get all roles
   */
  getRoles(): RoleInfo[] {
    return Array.from(this.roles.values());
  }

  /**
   * Search roles by prefix
   */
  searchRoles(prefix: string): RoleInfo[] {
    const lowerPrefix = prefix.toLowerCase();
    return this.getRoles().filter(role =>
      role.name.toLowerCase().startsWith(lowerPrefix)
    );
  }

  /**
   * Load users into cache
   */
  loadUsers(users: UserInfo[]): void {
    this.users.clear();
    for (const user of users) {
      this.users.set(user.name.toUpperCase(), user);
    }
    console.log(`Loaded ${users.length} users into cache`);
  }

  /**
   * Get all users
   */
  getUsers(): UserInfo[] {
    return Array.from(this.users.values());
  }

  /**
   * Search users by prefix
   */
  searchUsers(prefix: string): UserInfo[] {
    const lowerPrefix = prefix.toLowerCase();
    return this.getUsers().filter(user =>
      user.name.toLowerCase().startsWith(lowerPrefix) ||
      user.login_name.toLowerCase().startsWith(lowerPrefix)
    );
  }

  /**
   * Load databases into cache
   */
  loadDatabases(databases: DatabaseInfo[]): void {
    this.databases.clear();
    for (const db of databases) {
      this.databases.set(db.name.toUpperCase(), db);
    }
    console.log(`Loaded ${databases.length} databases into cache`);
  }

  /**
   * Get all databases
   */
  getDatabases(): DatabaseInfo[] {
    return Array.from(this.databases.values());
  }

  /**
   * Search databases by prefix
   */
  searchDatabases(prefix: string): DatabaseInfo[] {
    const lowerPrefix = prefix.toLowerCase();
    return this.getDatabases().filter(db =>
      db.name.toLowerCase().startsWith(lowerPrefix)
    );
  }
}
