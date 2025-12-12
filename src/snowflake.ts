import snowflake from 'snowflake-sdk';

export interface SnowflakeConfig {
  account: string;
  user: string;
  database: string;
  warehouse: string;
  role?: string;
  schema?: string;
}

export interface TableInfo {
  catalog: string;
  schema: string;
  name: string;
  type: string; // 'BASE TABLE' or 'VIEW'

  // Enhanced metadata from SHOW TABLES
  comment: string | null;
  owner: string;
  rows: number | null;
  bytes: number | null;
  created_on: string;
  kind: string; // TABLE, TEMPORARY, TRANSIENT, EXTERNAL
  retention_time: number;
  is_external: boolean;
  cluster_by: string | null;
}

export interface ColumnInfo {
  catalog: string;
  schema: string;
  tableName: string;
  columnName: string;
  dataType: string;
  isNullable: string;
  columnDefault: string | null;

  // Enhanced metadata from SHOW COLUMNS
  comment: string | null;
  kind: string;
  autoincrement: string | null;
}

export interface ViewInfo {
  catalog: string;
  schema: string;
  name: string;
  definition: string;
}

export interface DatabaseInfo {
  name: string;
  created_on: string;
  owner: string;
  comment: string | null;
  retention_time: number;
}

export interface SchemaInfo {
  name: string;
  database_name: string;
  created_on: string;
  owner: string;
  comment: string | null;
}

export interface DDLInfo {
  qualifiedName: string;
  ddl: string;
  fetchedAt: number; // timestamp for TTL
}

export interface WarehouseInfo {
  name: string;
  state: string; // STARTED, SUSPENDED, RESUMING
  type: string; // STANDARD, SNOWPARK-OPTIMIZED
  size: string; // X-SMALL, SMALL, MEDIUM, LARGE, X-LARGE, etc.
  running: number; // number of running queries
  queued: number; // number of queued queries
  is_default: boolean;
  is_current: boolean;
  auto_suspend: number | null; // minutes
  auto_resume: boolean;
  available: string; // available percentage
  provisioning: string;
  quiescing: string;
  other: string;
  created_on: string;
  resumed_on: string;
  updated_on: string;
  owner: string;
  comment: string | null;
  resource_monitor: string;
  actives: number;
  pendings: number;
  failed: number;
  suspended: number;
  uuid: string;
  scaling_policy: string;
}

export interface RoleInfo {
  created_on: string;
  name: string;
  is_default: boolean;
  is_current: boolean;
  is_inherited: boolean;
  assigned_to_users: number;
  granted_to_roles: number;
  granted_roles: number;
  owner: string;
  comment: string | null;
}

export interface UserInfo {
  name: string;
  created_on: string;
  login_name: string;
  display_name: string;
  first_name: string;
  last_name: string;
  email: string;
  disabled: boolean;
  must_change_password: boolean;
  snowflake_lock: boolean;
  default_warehouse: string | null;
  default_namespace: string | null;
  default_role: string | null;
  ext_authn_duo: boolean;
  ext_authn_uid: string | null;
  mins_to_bypass_mfa: number;
  owner: string;
  comment: string | null;
}

export interface QueryHistoryInfo {
  query_id: string;
  query_text: string;
  database_name: string;
  schema_name: string;
  query_type: string;
  session_id: number;
  user_name: string;
  role_name: string;
  warehouse_name: string;
  warehouse_size: string;
  warehouse_type: string;
  cluster_number: number;
  query_tag: string;
  execution_status: string;
  error_code: string | null;
  error_message: string | null;
  start_time: string;
  end_time: string;
  total_elapsed_time: number;
  bytes_scanned: number;
  rows_produced: number;
  compilation_time: number;
  execution_time: number;
  queued_provisioning_time: number;
  queued_repair_time: number;
  queued_overload_time: number;
  transaction_blocked_time: number;
}

export class SnowflakeConnection {
  private connection: snowflake.Connection | null = null;
  private config: SnowflakeConfig;
  private isConnected: boolean = false;
  private useShowCommands: boolean = true; // Try SHOW first, fallback to INFORMATION_SCHEMA

  constructor(config: SnowflakeConfig) {
    this.config = config;
  }

  /**
   * Establish connection to Snowflake using external browser authentication
   */
  async connect(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.connection = snowflake.createConnection({
        account: this.config.account,
        username: this.config.user,
        database: this.config.database,
        warehouse: this.config.warehouse,
        role: this.config.role,
        schema: this.config.schema,
        authenticator: 'EXTERNALBROWSER',
      });

      this.connection.connectAsync((err, conn) => {
        if (err) {
          console.error('Failed to connect to Snowflake:', err);
          reject(err);
        } else {
          console.log('Successfully connected to Snowflake');
          this.isConnected = true;
          resolve();
        }
      });
    });
  }

  /**
   * Execute a query and return results
   */
  private async executeQuery<T>(query: string): Promise<T[]> {
    if (!this.connection || !this.isConnected) {
      throw new Error('Not connected to Snowflake');
    }

    return new Promise((resolve, reject) => {
      this.connection!.execute({
        sqlText: query,
        complete: (err, stmt, rows) => {
          if (err) {
            console.error('Query execution failed:', err);
            reject(err);
          } else {
            resolve(rows as T[]);
          }
        },
      });
    });
  }

  /**
   * Fetch schemas using SHOW SCHEMAS IN DATABASE
   */
  async fetchSchemas(database: string): Promise<SchemaInfo[]> {
    const query = `SHOW SCHEMAS IN DATABASE ${database}`;

    try {
      const rows = await this.executeQuery<any>(query);
      // SHOW commands return lowercase column names
      return rows.map(row => ({
        name: row.name,
        database_name: row.database_name,
        created_on: row.created_on,
        owner: row.owner,
        comment: row.comment || null,
      }));
    } catch (error) {
      console.error(`SHOW SCHEMAS IN DATABASE ${database} failed:`, error);
      throw error;
    }
  }

  /**
   * Fetch tables using SHOW TABLES IN SCHEMA
   */
  private async fetchTablesViaShow(database: string, schema: string): Promise<TableInfo[]> {
    const query = `SHOW TABLES IN SCHEMA ${database}.${schema}`;

    try {
      const rows = await this.executeQuery<any>(query);
      // SHOW commands return lowercase column names
      return rows.map(row => ({
        catalog: row.database_name,
        schema: row.schema_name,
        name: row.name,
        type: row.kind === 'VIEW' ? 'VIEW' : 'BASE TABLE',

        // Enhanced metadata from SHOW TABLES
        comment: row.comment || null,
        owner: row.owner,
        rows: row.rows !== null ? Number(row.rows) : null,
        bytes: row.bytes !== null ? Number(row.bytes) : null,
        created_on: row.created_on,
        kind: row.kind,
        retention_time: Number(row.retention_time) || 1,
        is_external: row.is_external === 'Y',
        cluster_by: row.cluster_by || null,
      }));
    } catch (error) {
      console.error(`SHOW TABLES IN SCHEMA ${database}.${schema} failed:`, error);
      throw error;
    }
  }

  /**
   * Fetch all tables (public method with fallback)
   */
  async fetchTables(): Promise<TableInfo[]> {
    if (this.useShowCommands) {
      try {
        // Get current database and all schemas
        const database = this.config.database;
        const schemas = await this.fetchSchemas(database);

        // Fetch tables for all schemas
        const allTables: TableInfo[] = [];
        for (const schema of schemas) {
          if (schema.name === 'INFORMATION_SCHEMA') continue;
          const tables = await this.fetchTablesViaShow(database, schema.name);
          allTables.push(...tables);
        }

        return allTables;
      } catch (error) {
        console.error('SHOW TABLES failed, falling back to INFORMATION_SCHEMA');
        this.useShowCommands = false;
        return await this.fetchTablesViaInformationSchema();
      }
    } else {
      return await this.fetchTablesViaInformationSchema();
    }
  }

  /**
   * Fetch all tables from INFORMATION_SCHEMA (fallback method)
   */
  private async fetchTablesViaInformationSchema(): Promise<TableInfo[]> {
    const query = `
      SELECT
        table_catalog,
        table_schema,
        table_name,
        table_type
      FROM information_schema.tables
      WHERE table_schema NOT IN ('INFORMATION_SCHEMA')
      ORDER BY table_schema, table_name
    `;

    const rows = await this.executeQuery<any>(query);
    return rows.map(row => ({
      catalog: row.TABLE_CATALOG,
      schema: row.TABLE_SCHEMA,
      name: row.TABLE_NAME,
      type: row.TABLE_TYPE,

      // Default values for enhanced metadata
      comment: null,
      owner: 'UNKNOWN',
      rows: null,
      bytes: null,
      created_on: new Date().toISOString(),
      kind: row.TABLE_TYPE === 'VIEW' ? 'VIEW' : 'TABLE',
      retention_time: 1,
      is_external: false,
      cluster_by: null,
    }));
  }

  /**
   * Parse JSON data_type from SHOW COLUMNS
   * Example: {"type": "VARCHAR", "length": 255, "nullable": true}
   */
  private parseDataType(dataTypeJson: any): string {
    if (typeof dataTypeJson === 'string') {
      try {
        const parsed = JSON.parse(dataTypeJson);
        if (parsed.type) {
          if (parsed.length) {
            return `${parsed.type}(${parsed.length})`;
          } else if (parsed.precision !== undefined) {
            if (parsed.scale !== undefined) {
              return `${parsed.type}(${parsed.precision},${parsed.scale})`;
            }
            return `${parsed.type}(${parsed.precision})`;
          }
          return parsed.type;
        }
      } catch (error) {
        // If parsing fails, return as-is
        return String(dataTypeJson);
      }
    }
    return String(dataTypeJson);
  }

  /**
   * Fetch columns for a specific table using SHOW COLUMNS
   */
  async fetchColumnsForTable(database: string, schema: string, table: string): Promise<ColumnInfo[]> {
    const query = `SHOW COLUMNS IN TABLE ${database}.${schema}.${table}`;

    try {
      const rows = await this.executeQuery<any>(query);
      // SHOW commands return lowercase column names
      return rows.map(row => ({
        catalog: row.database_name,
        schema: row.schema_name,
        tableName: row.table_name,
        columnName: row.column_name,
        dataType: this.parseDataType(row.data_type),
        isNullable: row['null?'] === 'Y' ? 'YES' : 'NO',
        columnDefault: row.default || null,

        // Enhanced metadata from SHOW COLUMNS
        comment: row.comment || null,
        kind: row.kind || 'COLUMN',
        autoincrement: row.autoincrement || null,
      }));
    } catch (error) {
      console.error(`SHOW COLUMNS IN TABLE ${database}.${schema}.${table} failed:`, error);
      throw error;
    }
  }

  /**
   * Fetch all columns (public method with fallback)
   * Note: This method is kept for backward compatibility but lazy loading is preferred
   */
  async fetchColumns(): Promise<ColumnInfo[]> {
    if (this.useShowCommands) {
      try {
        // Get all tables first
        const tables = await this.fetchTables();

        // Fetch columns for each table
        const allColumns: ColumnInfo[] = [];
        for (const table of tables) {
          const columns = await this.fetchColumnsForTable(
            table.catalog,
            table.schema,
            table.name
          );
          allColumns.push(...columns);
        }

        return allColumns;
      } catch (error) {
        console.error('SHOW COLUMNS failed, falling back to INFORMATION_SCHEMA');
        this.useShowCommands = false;
        return await this.fetchColumnsViaInformationSchema();
      }
    } else {
      return await this.fetchColumnsViaInformationSchema();
    }
  }

  /**
   * Fetch all columns from INFORMATION_SCHEMA (fallback method)
   */
  private async fetchColumnsViaInformationSchema(): Promise<ColumnInfo[]> {
    const query = `
      SELECT
        table_catalog,
        table_schema,
        table_name,
        column_name,
        data_type,
        is_nullable,
        column_default
      FROM information_schema.columns
      WHERE table_schema NOT IN ('INFORMATION_SCHEMA')
      ORDER BY table_schema, table_name, ordinal_position
    `;

    const rows = await this.executeQuery<any>(query);
    return rows.map(row => ({
      catalog: row.TABLE_CATALOG,
      schema: row.TABLE_SCHEMA,
      tableName: row.TABLE_NAME,
      columnName: row.COLUMN_NAME,
      dataType: row.DATA_TYPE,
      isNullable: row.IS_NULLABLE,
      columnDefault: row.COLUMN_DEFAULT,

      // Default values for enhanced metadata
      comment: null,
      kind: 'COLUMN',
      autoincrement: null,
    }));
  }

  /**
   * Fetch DDL for a specific table using GET_DDL
   * Returns DDL string or throws error
   */
  async fetchDDL(database: string, schema: string, table: string): Promise<string> {
    const qualifiedName = `${database}.${schema}.${table}`;
    const query = `SELECT GET_DDL('TABLE', '${qualifiedName}')`;

    try {
      const rows = await this.executeQuery<any>(query);
      if (rows && rows.length > 0) {
        // GET_DDL returns a single column with the DDL
        const firstRow = rows[0];
        const ddl = firstRow[Object.keys(firstRow)[0]]; // Get first column value
        return ddl || 'DDL not available';
      }
      return 'DDL not available';
    } catch (error) {
      console.error(`GET_DDL for ${qualifiedName} failed:`, error);
      throw error;
    }
  }

  /**
   * Fetch all views from INFORMATION_SCHEMA
   */
  async fetchViews(): Promise<ViewInfo[]> {
    const query = `
      SELECT
        table_catalog,
        table_schema,
        table_name,
        view_definition
      FROM information_schema.views
      WHERE table_schema NOT IN ('INFORMATION_SCHEMA')
      ORDER BY table_schema, table_name
    `;

    const rows = await this.executeQuery<any>(query);
    return rows.map(row => ({
      catalog: row.TABLE_CATALOG,
      schema: row.TABLE_SCHEMA,
      name: row.TABLE_NAME,
      definition: row.VIEW_DEFINITION,
    }));
  }

  /**
   * Disconnect from Snowflake
   */
  async disconnect(): Promise<void> {
    if (this.connection && this.isConnected) {
      return new Promise((resolve, reject) => {
        this.connection!.destroy((err) => {
          if (err) {
            console.error('Error disconnecting from Snowflake:', err);
            reject(err);
          } else {
            console.log('Disconnected from Snowflake');
            this.isConnected = false;
            this.connection = null;
            resolve();
          }
        });
      });
    }
  }

  /**
   * Check if connected
   */
  isConnectionActive(): boolean {
    return this.isConnected;
  }

  /**
   * Fetch all warehouses using SHOW WAREHOUSES
   */
  async fetchWarehouses(): Promise<WarehouseInfo[]> {
    const query = 'SHOW WAREHOUSES';

    try {
      const rows = await this.executeQuery<any>(query);
      return rows.map(row => ({
        name: row.name,
        state: row.state,
        type: row.type,
        size: row.size,
        running: Number(row.running) || 0,
        queued: Number(row.queued) || 0,
        is_default: row.is_default === 'Y',
        is_current: row.is_current === 'Y',
        auto_suspend: row.auto_suspend !== null ? Number(row.auto_suspend) : null,
        auto_resume: row.auto_resume === 'true',
        available: row.available || '',
        provisioning: row.provisioning || '',
        quiescing: row.quiescing || '',
        other: row.other || '',
        created_on: row.created_on,
        resumed_on: row.resumed_on,
        updated_on: row.updated_on,
        owner: row.owner,
        comment: row.comment || null,
        resource_monitor: row.resource_monitor,
        actives: Number(row.actives) || 0,
        pendings: Number(row.pendings) || 0,
        failed: Number(row.failed) || 0,
        suspended: Number(row.suspended) || 0,
        uuid: row.uuid,
        scaling_policy: row.scaling_policy,
      }));
    } catch (error) {
      console.error('SHOW WAREHOUSES failed:', error);
      return [];
    }
  }

  /**
   * Fetch all roles using SHOW ROLES
   */
  async fetchRoles(): Promise<RoleInfo[]> {
    const query = 'SHOW ROLES';

    try {
      const rows = await this.executeQuery<any>(query);
      return rows.map(row => ({
        created_on: row.created_on,
        name: row.name,
        is_default: row.is_default === 'Y',
        is_current: row.is_current === 'Y',
        is_inherited: row.is_inherited === 'Y',
        assigned_to_users: Number(row.assigned_to_users) || 0,
        granted_to_roles: Number(row.granted_to_roles) || 0,
        granted_roles: Number(row.granted_roles) || 0,
        owner: row.owner,
        comment: row.comment || null,
      }));
    } catch (error) {
      console.error('SHOW ROLES failed:', error);
      return [];
    }
  }

  /**
   * Fetch all users using SHOW USERS
   */
  async fetchUsers(): Promise<UserInfo[]> {
    const query = 'SHOW USERS';

    try {
      const rows = await this.executeQuery<any>(query);
      return rows.map(row => ({
        name: row.name,
        created_on: row.created_on,
        login_name: row.login_name,
        display_name: row.display_name,
        first_name: row.first_name || '',
        last_name: row.last_name || '',
        email: row.email || '',
        disabled: row.disabled === 'true',
        must_change_password: row.must_change_password === 'true',
        snowflake_lock: row.snowflake_lock === 'true',
        default_warehouse: row.default_warehouse || null,
        default_namespace: row.default_namespace || null,
        default_role: row.default_role || null,
        ext_authn_duo: row.ext_authn_duo === 'true',
        ext_authn_uid: row.ext_authn_uid || null,
        mins_to_bypass_mfa: Number(row.mins_to_bypass_mfa) || 0,
        owner: row.owner,
        comment: row.comment || null,
      }));
    } catch (error) {
      console.error('SHOW USERS failed:', error);
      return [];
    }
  }

  /**
   * Fetch all databases using SHOW DATABASES
   */
  async fetchDatabases(): Promise<DatabaseInfo[]> {
    const query = 'SHOW DATABASES';

    try {
      const rows = await this.executeQuery<any>(query);
      return rows.map(row => ({
        name: row.name,
        created_on: row.created_on,
        owner: row.owner,
        comment: row.comment || null,
        retention_time: Number(row.retention_time) || 1,
      }));
    } catch (error) {
      console.error('SHOW DATABASES failed:', error);
      return [];
    }
  }

  /**
   * Fetch recent query history using INFORMATION_SCHEMA.QUERY_HISTORY()
   * Returns last 50 successful queries
   */
  async fetchQueryHistory(): Promise<QueryHistoryInfo[]> {
    const query = `
      SELECT
        query_id,
        query_text,
        database_name,
        schema_name,
        query_type,
        session_id,
        user_name,
        role_name,
        warehouse_name,
        warehouse_size,
        warehouse_type,
        cluster_number,
        query_tag,
        execution_status,
        error_code,
        error_message,
        start_time,
        end_time,
        total_elapsed_time,
        bytes_scanned,
        rows_produced,
        compilation_time,
        execution_time,
        queued_provisioning_time,
        queued_repair_time,
        queued_overload_time,
        transaction_blocked_time
      FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
      WHERE execution_status = 'SUCCESS'
        AND query_type != 'SHOW'
        AND query_text NOT LIKE '%QUERY_HISTORY%'
      ORDER BY start_time DESC
      LIMIT 50
    `;

    try {
      const rows = await this.executeQuery<any>(query);
      return rows.map(row => ({
        query_id: row.QUERY_ID || row.query_id,
        query_text: row.QUERY_TEXT || row.query_text,
        database_name: row.DATABASE_NAME || row.database_name,
        schema_name: row.SCHEMA_NAME || row.schema_name,
        query_type: row.QUERY_TYPE || row.query_type,
        session_id: Number(row.SESSION_ID || row.session_id) || 0,
        user_name: row.USER_NAME || row.user_name,
        role_name: row.ROLE_NAME || row.role_name,
        warehouse_name: row.WAREHOUSE_NAME || row.warehouse_name,
        warehouse_size: row.WAREHOUSE_SIZE || row.warehouse_size || '',
        warehouse_type: row.WAREHOUSE_TYPE || row.warehouse_type || '',
        cluster_number: Number(row.CLUSTER_NUMBER || row.cluster_number) || 0,
        query_tag: row.QUERY_TAG || row.query_tag || '',
        execution_status: row.EXECUTION_STATUS || row.execution_status,
        error_code: row.ERROR_CODE || row.error_code || null,
        error_message: row.ERROR_MESSAGE || row.error_message || null,
        start_time: row.START_TIME || row.start_time,
        end_time: row.END_TIME || row.end_time,
        total_elapsed_time: Number(row.TOTAL_ELAPSED_TIME || row.total_elapsed_time) || 0,
        bytes_scanned: Number(row.BYTES_SCANNED || row.bytes_scanned) || 0,
        rows_produced: Number(row.ROWS_PRODUCED || row.rows_produced) || 0,
        compilation_time: Number(row.COMPILATION_TIME || row.compilation_time) || 0,
        execution_time: Number(row.EXECUTION_TIME || row.execution_time) || 0,
        queued_provisioning_time: Number(row.QUEUED_PROVISIONING_TIME || row.queued_provisioning_time) || 0,
        queued_repair_time: Number(row.QUEUED_REPAIR_TIME || row.queued_repair_time) || 0,
        queued_overload_time: Number(row.QUEUED_OVERLOAD_TIME || row.queued_overload_time) || 0,
        transaction_blocked_time: Number(row.TRANSACTION_BLOCKED_TIME || row.transaction_blocked_time) || 0,
      }));
    } catch (error) {
      console.error('QUERY_HISTORY() failed:', error);
      return [];
    }
  }
}

/**
 * Load Snowflake configuration from environment variables
 */
export function loadConfigFromEnv(): SnowflakeConfig {
  const account = process.env.SNOWFLAKE_ACCOUNT;
  const user = process.env.SNOWFLAKE_USER;
  const database = process.env.SNOWFLAKE_DATABASE;
  const warehouse = process.env.SNOWFLAKE_WAREHOUSE;

  if (!account || !user || !database || !warehouse) {
    throw new Error(
      'Missing required environment variables: ' +
      'SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_DATABASE, SNOWFLAKE_WAREHOUSE'
    );
  }

  return {
    account,
    user,
    database,
    warehouse,
    role: process.env.SNOWFLAKE_ROLE,
    schema: process.env.SNOWFLAKE_SCHEMA,
  };
}
