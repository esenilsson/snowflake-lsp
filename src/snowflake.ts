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
   * Fetch databases using SHOW DATABASES
   */
  async fetchDatabases(): Promise<DatabaseInfo[]> {
    const query = 'SHOW DATABASES';

    try {
      const rows = await this.executeQuery<any>(query);
      // SHOW commands return lowercase column names
      return rows.map(row => ({
        name: row.name,
        created_on: row.created_on,
        owner: row.owner,
        comment: row.comment || null,
        retention_time: row.retention_time || 1,
      }));
    } catch (error) {
      console.error('SHOW DATABASES failed:', error);
      throw error;
    }
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
