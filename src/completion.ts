import { CompletionItem, CompletionItemKind, TextDocumentPositionParams } from 'vscode-languageserver/node';
import { TextDocument } from 'vscode-languageserver-textdocument';
import { SchemaCache } from './schema-cache';
import { parseContext, SQLContext, getSQLKeywords } from './sql-parser';
import { SessionContext } from './session-context';

export class CompletionProvider {
  constructor(private schemaCache: SchemaCache) {}

  /**
   * Provide completion items based on cursor position
   */
  provideCompletions(
    document: TextDocument,
    params: TextDocumentPositionParams,
    context?: SessionContext
  ): CompletionItem[] {
    try {
      const text = document.getText();
      const offset = document.offsetAt(params.position);

      // Parse the SQL context
      const parsed = parseContext(text, offset);

      // Check for query history trigger patterns
      const beforeCursor = text.substring(0, offset);
      const lastLine = beforeCursor.split('\n').pop() || '';

      // If line starts with comment and contains 'history', show query history
      if (/^\s*--\s*history/i.test(lastLine) || /\bqh:/i.test(lastLine)) {
        return this.getQueryHistoryCompletions(parsed.currentWord);
      }

      // Generate completions based on context
      const completions: CompletionItem[] = [];

      try {
        switch (parsed.context) {
          case SQLContext.FROM_CLAUSE:
            // Suggest table names and schemas, prioritized by session context if available
            if (context) {
              completions.push(...this.getTableCompletionsWithContext(parsed.currentWord, context));
            } else {
              completions.push(...this.getTableCompletions(parsed.currentWord));
            }
            completions.push(...this.getSchemaCompletions(parsed.currentWord));
            break;

          case SQLContext.TABLE_DOT:
            // Suggest columns for the table or alias
            completions.push(...this.getColumnCompletionsForTable(parsed.currentWord, parsed.aliases));
            break;

          case SQLContext.SCHEMA_DOT:
            // Suggest tables in schema
            completions.push(...this.getTableCompletionsForSchema(parsed.currentWord));
            break;

          case SQLContext.USE_WAREHOUSE:
            // Suggest warehouse names
            completions.push(...this.getWarehouseCompletions(parsed.currentWord));
            break;

          case SQLContext.USE_ROLE:
            // Suggest role names
            completions.push(...this.getRoleCompletions(parsed.currentWord));
            break;

          case SQLContext.USE_DATABASE:
            // Suggest database names
            completions.push(...this.getDatabaseCompletions(parsed.currentWord));
            break;

          case SQLContext.GRANT_TO_ROLE:
            // Suggest role names for GRANT ... TO ROLE
            completions.push(...this.getRoleCompletions(parsed.currentWord));
            break;

          case SQLContext.GRANT_TO_USER:
            // Suggest user names for GRANT ... TO USER
            completions.push(...this.getUserCompletions(parsed.currentWord));
            break;

          case SQLContext.SELECT_LIST:
          case SQLContext.WHERE_CLAUSE:
            // Suggest columns from tables in scope and SQL keywords
            completions.push(...this.getColumnCompletions(parsed.currentWord, parsed.tablesInScope));
            completions.push(...this.getKeywordCompletions(parsed.currentWord));
            break;

          case SQLContext.GENERAL:
          default:
            // Suggest SQL keywords, tables, schemas, and columns
            completions.push(...this.getKeywordCompletions(parsed.currentWord));
            if (context) {
              completions.push(...this.getTableCompletionsWithContext(parsed.currentWord, context));
            } else {
              completions.push(...this.getTableCompletions(parsed.currentWord));
            }
            completions.push(...this.getSchemaCompletions(parsed.currentWord));
            break;
        }
      } catch (error) {
        console.error('Error generating completions for context:', error);
        // Return what we have so far
      }

      return completions;
    } catch (error) {
      console.error('Completion provider error:', error);
      return [];
    }
  }

  /**
   * Get table name completions
   */
  private getTableCompletions(prefix: string): CompletionItem[] {
    const tables = this.schemaCache.searchTables(prefix, 50);

    return tables.map(table => ({
      label: table.info.name,
      kind: CompletionItemKind.Class,
      detail: `${table.info.schema}.${table.info.name}`,
      documentation: `Table in ${table.info.schema} schema\nType: ${table.info.type}\nColumns: ${table.columns.length}`,
      insertText: table.info.name,
    }));
  }

  /**
   * Get table name completions with session context prioritization
   * Prioritizes tables from current schema (✓), then current database (•), then others
   */
  private getTableCompletionsWithContext(prefix: string, context: SessionContext): CompletionItem[] {
    const allTables = this.schemaCache.searchTables(prefix, 100);

    // Separate into 3 priority groups
    const currentSchema: CompletionItem[] = [];
    const currentDatabase: CompletionItem[] = [];
    const others: CompletionItem[] = [];

    for (const table of allTables) {
      const item: CompletionItem = {
        label: table.info.name,
        kind: CompletionItemKind.Class,
        detail: `${table.info.catalog}.${table.info.schema}.${table.info.name}`,
        documentation: `Table in ${table.info.schema} schema\nType: ${table.info.type}\nColumns: ${table.columns.length}`,
        insertText: table.info.name,
      };

      // Check if table is in current schema (highest priority)
      if (context.database && context.schema &&
          table.info.catalog.toUpperCase() === context.database.toUpperCase() &&
          table.info.schema.toUpperCase() === context.schema.toUpperCase()) {
        item.sortText = `0_${table.info.name}`;
        item.documentation = `✓ Current schema\n${item.documentation}`;
        currentSchema.push(item);
      }
      // Check if table is in current database (medium priority)
      else if (context.database &&
               table.info.catalog.toUpperCase() === context.database.toUpperCase()) {
        item.sortText = `1_${table.info.name}`;
        item.documentation = `• Current database\n${item.documentation}`;
        currentDatabase.push(item);
      }
      // Other tables (lowest priority)
      else {
        item.sortText = `2_${table.info.name}`;
        others.push(item);
      }
    }

    // Return with current schema first, then current database, then others
    return [...currentSchema, ...currentDatabase, ...others];
  }

  /**
   * Get schema name completions
   */
  private getSchemaCompletions(prefix: string): CompletionItem[] {
    const schemas = this.schemaCache.searchSchemas(prefix);

    return schemas.map(schema => ({
      label: schema,
      kind: CompletionItemKind.Module,
      detail: `Schema: ${schema}`,
      documentation: `Database schema`,
      insertText: schema,
    }));
  }

  /**
   * Get tables for a specific schema
   */
  private getTableCompletionsForSchema(schemaPrefix: string): CompletionItem[] {
    // Extract schema name from prefix (e.g., "SCHEMA_NAME.")
    const parts = schemaPrefix.split('.');
    if (parts.length < 1) return [];

    const schemaName = parts[0];
    const tablePrefix = parts[1] || '';

    const tables = this.schemaCache.searchTables(tablePrefix);
    const filtered = tables.filter(t =>
      t.info.schema.toUpperCase() === schemaName.toUpperCase()
    );

    return filtered.map(table => ({
      label: table.info.name,
      kind: CompletionItemKind.Class,
      detail: `${table.info.schema}.${table.info.name}`,
      documentation: `Table in ${table.info.schema} schema\nColumns: ${table.columns.length}`,
      insertText: table.info.name,
    }));
  }

  /**
   * Get column completions
   */
  private getColumnCompletions(prefix: string, tablesInScope: string[]): CompletionItem[] {
    const columns = this.schemaCache.searchColumns(prefix, tablesInScope, 50);

    return columns.map(column => ({
      label: column.info.columnName,
      kind: CompletionItemKind.Field,
      detail: `${column.info.tableName}.${column.info.columnName}`,
      documentation: `Column: ${column.info.dataType}\nTable: ${column.info.tableName}\nNullable: ${column.info.isNullable}`,
      insertText: column.info.columnName,
    }));
  }

  /**
   * Get column completions for a specific table or alias (after table. or alias.)
   */
  private getColumnCompletionsForTable(tablePrefix: string, aliases: Map<string, string>): CompletionItem[] {
    // Extract identifier from prefix (e.g., "a." -> "a")
    const parts = tablePrefix.split('.');
    if (parts.length < 1) return [];

    const identifier = parts[0];

    // Check if it's an alias first
    const actualTableName = aliases.get(identifier.toLowerCase()) || identifier;

    // Try to find the table
    const table = this.schemaCache.getTable(actualTableName);
    if (!table) {
      return [];
    }

    return this.createColumnCompletionsFromTable(table.columns);
  }

  /**
   * Create completion items from column info array
   */
  private createColumnCompletionsFromTable(columns: any[]): CompletionItem[] {
    return columns.map(column => ({
      label: column.columnName,
      kind: CompletionItemKind.Field,
      detail: column.dataType,
      documentation: `Type: ${column.dataType}\nNullable: ${column.isNullable}${column.columnDefault ? `\nDefault: ${column.columnDefault}` : ''}`,
      insertText: column.columnName,
    }));
  }

  /**
   * Get SQL keyword completions
   */
  private getKeywordCompletions(prefix: string): CompletionItem[] {
    const keywords = getSQLKeywords();
    const lowerPrefix = prefix.toLowerCase();

    return keywords
      .filter(kw => kw.toLowerCase().startsWith(lowerPrefix))
      .map(keyword => ({
        label: keyword,
        kind: CompletionItemKind.Keyword,
        insertText: keyword,
      }));
  }

  /**
   * Get warehouse completions
   */
  private getWarehouseCompletions(prefix: string): CompletionItem[] {
    const warehouses = this.schemaCache.searchWarehouses(prefix);

    return warehouses.map(warehouse => ({
      label: warehouse.name,
      kind: CompletionItemKind.Constant,
      detail: `${warehouse.size} (${warehouse.state})`,
      documentation: `Warehouse: ${warehouse.name}\nSize: ${warehouse.size}\nState: ${warehouse.state}\nAuto Suspend: ${warehouse.auto_suspend || 'N/A'} min\nAuto Resume: ${warehouse.auto_resume}`,
      insertText: warehouse.name,
    }));
  }

  /**
   * Get role completions
   */
  private getRoleCompletions(prefix: string): CompletionItem[] {
    const roles = this.schemaCache.searchRoles(prefix);

    return roles.map(role => ({
      label: role.name,
      kind: CompletionItemKind.EnumMember,
      detail: role.is_current ? `${role.name} (current)` : role.name,
      documentation: `Role: ${role.name}\nAssigned to ${role.assigned_to_users} user(s)\nGranted to ${role.granted_to_roles} role(s)\nOwner: ${role.owner}`,
      insertText: role.name,
    }));
  }

  /**
   * Get user completions
   */
  private getUserCompletions(prefix: string): CompletionItem[] {
    const users = this.schemaCache.searchUsers(prefix);

    return users.map(user => ({
      label: user.name,
      kind: CompletionItemKind.Value,
      detail: user.display_name || user.name,
      documentation: `User: ${user.name}\nLogin: ${user.login_name}\nEmail: ${user.email}\nDisabled: ${user.disabled}`,
      insertText: user.name,
    }));
  }

  /**
   * Get database completions
   */
  private getDatabaseCompletions(prefix: string): CompletionItem[] {
    const databases = this.schemaCache.searchDatabases(prefix);

    return databases.map(db => ({
      label: db.name,
      kind: CompletionItemKind.Module,
      detail: `Database: ${db.name}`,
      documentation: `Database: ${db.name}\nOwner: ${db.owner}\nCreated: ${db.created_on}\nRetention: ${db.retention_time} day(s)${db.comment ? `\nComment: ${db.comment}` : ''}`,
      insertText: db.name,
    }));
  }

  /**
   * Get query history completions
   */
  private getQueryHistoryCompletions(prefix: string): CompletionItem[] {
    const queries = this.schemaCache.searchQueryHistory(prefix, 20);

    return queries.map((query, index) => {
      // Truncate query text for label (first 60 chars)
      const truncatedQuery = query.query_text.length > 60
        ? query.query_text.substring(0, 60) + '...'
        : query.query_text;

      // Format execution time
      const executionSeconds = (query.total_elapsed_time / 1000).toFixed(2);

      // Format timestamp
      const timestamp = new Date(query.start_time).toLocaleString();

      return {
        label: `${index + 1}. ${truncatedQuery}`,
        kind: CompletionItemKind.Snippet,
        detail: `${timestamp} (${executionSeconds}s)`,
        documentation: `Query ID: ${query.query_id}\n` +
                      `Database: ${query.database_name}.${query.schema_name}\n` +
                      `User: ${query.user_name}\n` +
                      `Warehouse: ${query.warehouse_name} (${query.warehouse_size})\n` +
                      `Execution Time: ${executionSeconds}s\n` +
                      `Rows Produced: ${query.rows_produced.toLocaleString()}\n` +
                      `Bytes Scanned: ${this.formatBytes(query.bytes_scanned)}\n\n` +
                      `Full Query:\n${query.query_text}`,
        insertText: query.query_text,
        sortText: `${index.toString().padStart(3, '0')}`, // Keep chronological order
      };
    });
  }

  /**
   * Format bytes to human-readable format
   */
  private formatBytes(bytes: number): string {
    if (bytes === 0) return '0 B';

    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));

    return `${(bytes / Math.pow(k, i)).toFixed(2)} ${sizes[i]}`;
  }
}
