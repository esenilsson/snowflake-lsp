import { Hover, MarkupContent, TextDocumentPositionParams } from 'vscode-languageserver/node';
import { TextDocument } from 'vscode-languageserver-textdocument';
import { SchemaCache } from './schema-cache';
import { parseContext } from './sql-parser';

export class HoverProvider {
  constructor(private schemaCache: SchemaCache) {}

  /**
   * Provide hover information for symbol at cursor position
   */
  provideHover(
    document: TextDocument,
    params: TextDocumentPositionParams
  ): Hover | undefined {
    const text = document.getText();
    const offset = document.offsetAt(params.position);

    // Parse context to get current word
    const parsed = parseContext(text, offset);
    const word = parsed.currentWord;

    if (!word) return undefined;

    // Try to find as table
    const table = this.schemaCache.getTable(word);
    if (table) {
      return {
        contents: this.createTableHoverContent(table),
      };
    }

    // Try to find as column
    // Need to check tables in scope to determine which table the column belongs to
    for (const tableName of parsed.tablesInScope) {
      const columns = this.schemaCache.getTableColumns(tableName);
      const column = columns.find(c =>
        c.columnName.toLowerCase() === word.toLowerCase()
      );

      if (column) {
        return {
          contents: this.createColumnHoverContent(column),
        };
      }
    }

    // If not found in scope tables, search all columns
    const allColumns = this.schemaCache.searchColumns(word, undefined, 1);
    if (allColumns.length > 0) {
      return {
        contents: this.createColumnHoverContent(allColumns[0].info),
      };
    }

    return undefined;
  }

  /**
   * Create hover content for a table
   */
  private createTableHoverContent(table: any): MarkupContent {
    const columnList = table.columns
      .map((col: any) => `  - \`${col.columnName}\` (${col.dataType})`)
      .join('\n');

    const markdown = [
      `### Table: \`${table.info.name}\``,
      '',
      `**Database**: ${table.info.catalog}`,
      `**Schema**: ${table.info.schema}`,
      `**Type**: ${table.info.type}`,
      `**Columns**: ${table.columns.length}`,
      '',
      '#### Columns:',
      columnList,
    ].join('\n');

    return {
      kind: 'markdown',
      value: markdown,
    };
  }

  /**
   * Create hover content for a column
   */
  private createColumnHoverContent(column: any): MarkupContent {
    const markdown = [
      `### Column: \`${column.columnName}\``,
      '',
      `**Table**: ${column.tableName}`,
      `**Schema**: ${column.schema}`,
      `**Data Type**: ${column.dataType}`,
      `**Nullable**: ${column.isNullable}`,
      column.columnDefault ? `**Default**: ${column.columnDefault}` : '',
    ].filter(line => line !== '').join('\n');

    return {
      kind: 'markdown',
      value: markdown,
    };
  }
}
