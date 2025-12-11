import { CompletionItem, CompletionItemKind, TextDocumentPositionParams } from 'vscode-languageserver/node';
import { TextDocument } from 'vscode-languageserver-textdocument';
import { SchemaCache } from './schema-cache';
import { parseContext, SQLContext, getSQLKeywords } from './sql-parser';

export class CompletionProvider {
  constructor(private schemaCache: SchemaCache) {}

  /**
   * Provide completion items based on cursor position
   */
  provideCompletions(
    document: TextDocument,
    params: TextDocumentPositionParams
  ): CompletionItem[] {
    try {
      const text = document.getText();
      const offset = document.offsetAt(params.position);

      // Parse the SQL context
      const parsed = parseContext(text, offset);

      // Generate completions based on context
      const completions: CompletionItem[] = [];

      try {
        switch (parsed.context) {
          case SQLContext.FROM_CLAUSE:
            // Suggest table names and schemas
            completions.push(...this.getTableCompletions(parsed.currentWord));
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
            completions.push(...this.getTableCompletions(parsed.currentWord));
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
}
