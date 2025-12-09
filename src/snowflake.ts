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
}

export interface ColumnInfo {
  catalog: string;
  schema: string;
  tableName: string;
  columnName: string;
  dataType: string;
  isNullable: string;
  columnDefault: string | null;
}

export interface ViewInfo {
  catalog: string;
  schema: string;
  name: string;
  definition: string;
}

export class SnowflakeConnection {
  private connection: snowflake.Connection | null = null;
  private config: SnowflakeConfig;
  private isConnected: boolean = false;

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
   * Fetch all tables from INFORMATION_SCHEMA
   */
  async fetchTables(): Promise<TableInfo[]> {
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
    }));
  }

  /**
   * Fetch all columns from INFORMATION_SCHEMA
   */
  async fetchColumns(): Promise<ColumnInfo[]> {
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
    }));
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
