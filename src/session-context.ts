import { SnowflakeConnection } from './snowflake';

export interface SessionContext {
  uri: string;
  database: string | null;
  schema: string | null;
  warehouse: string | null;
  role: string | null;
  lastUpdated: number;
}

export enum UseCommandType {
  DATABASE = 'DATABASE',
  SCHEMA = 'SCHEMA',
  WAREHOUSE = 'WAREHOUSE',
  ROLE = 'ROLE',
}

export interface UseCommand {
  type: UseCommandType;
  value: string;
  line: number;
}

export class SessionContextManager {
  private contexts: Map<string, SessionContext> = new Map();
  private globalContext: SessionContext | null = null;

  /**
   * Initialize global context from Snowflake session
   * This is used as the default context for new documents
   */
  async initializeGlobalContext(connection: SnowflakeConnection): Promise<void> {
    try {
      const query = `
        SELECT
          CURRENT_DATABASE() as current_database,
          CURRENT_SCHEMA() as current_schema,
          CURRENT_WAREHOUSE() as current_warehouse,
          CURRENT_ROLE() as current_role
      `;

      const results = await (connection as any).executeQuery(query);

      if (results && results.length > 0) {
        const row = results[0];
        this.globalContext = {
          uri: '__global__',
          database: row.CURRENT_DATABASE || row.current_database || null,
          schema: row.CURRENT_SCHEMA || row.current_schema || null,
          warehouse: row.CURRENT_WAREHOUSE || row.current_warehouse || null,
          role: row.CURRENT_ROLE || row.current_role || null,
          lastUpdated: Date.now(),
        };

        console.log('Global session context initialized:', this.globalContext);
      }
    } catch (error) {
      console.error('Failed to initialize global context:', error);
      // Create default context from config
      this.globalContext = {
        uri: '__global__',
        database: null,
        schema: null,
        warehouse: null,
        role: null,
        lastUpdated: Date.now(),
      };
    }
  }

  /**
   * Get context for a document
   * If not exists, creates from global context
   */
  getContext(uri: string): SessionContext {
    if (!this.contexts.has(uri)) {
      // Create new context from global
      const newContext: SessionContext = {
        uri,
        database: this.globalContext?.database || null,
        schema: this.globalContext?.schema || null,
        warehouse: this.globalContext?.warehouse || null,
        role: this.globalContext?.role || null,
        lastUpdated: Date.now(),
      };
      this.contexts.set(uri, newContext);
      return newContext;
    }

    return this.contexts.get(uri)!;
  }

  /**
   * Update context based on USE commands
   * Commands are applied sequentially
   */
  updateContext(uri: string, commands: UseCommand[]): void {
    const context = this.getContext(uri);

    for (const cmd of commands) {
      switch (cmd.type) {
        case UseCommandType.DATABASE:
          context.database = cmd.value.toUpperCase();
          // Reset schema when database changes
          context.schema = null;
          break;

        case UseCommandType.SCHEMA:
          context.schema = cmd.value.toUpperCase();
          break;

        case UseCommandType.WAREHOUSE:
          context.warehouse = cmd.value.toUpperCase();
          break;

        case UseCommandType.ROLE:
          context.role = cmd.value.toUpperCase();
          break;
      }
    }

    context.lastUpdated = Date.now();
    this.contexts.set(uri, context);
  }

  /**
   * Clear context on document close
   */
  clearContext(uri: string): void {
    this.contexts.delete(uri);
  }

  /**
   * Get all active contexts (for debugging)
   */
  getActiveContexts(): Map<string, SessionContext> {
    return new Map(this.contexts);
  }
}
