import { Hover, MarkupContent, TextDocumentPositionParams } from 'vscode-languageserver/node';
import { TextDocument } from 'vscode-languageserver-textdocument';
import { SchemaCache } from './schema-cache';
import { parseContext } from './sql-parser';
import { SnowflakeConnection } from './snowflake';

export class HoverProvider {
  constructor(
    private schemaCache: SchemaCache,
    private snowflakeConnection: SnowflakeConnection
  ) {}

  /**
   * Provide hover information for symbol at cursor position
   */
  async provideHover(
    document: TextDocument,
    params: TextDocumentPositionParams
  ): Promise<Hover | undefined> {
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
        contents: await this.createTableHoverContent(table),
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
   * Format bytes to human-readable format
   */
  private formatBytes(bytes: number | null): string {
    if (bytes === null) return 'N/A';
    if (bytes === 0) return '0 B';

    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));

    return `${(bytes / Math.pow(k, i)).toFixed(2)} ${sizes[i]}`;
  }

  /**
   * Format number with commas
   */
  private formatNumber(num: number | null): string {
    if (num === null) return 'N/A';
    return num.toLocaleString();
  }

  /**
   * Create hover content for a table (async to fetch DDL)
   */
  private async createTableHoverContent(table: any): Promise<MarkupContent> {
    const info = table.info;
    const qualifiedName = `${info.catalog}.${info.schema}.${info.name}`;

    // Lazy-load DDL if not cached
    let ddl: string | undefined = this.schemaCache.getDDL(qualifiedName);
    if (!ddl) {
      try {
        ddl = await this.snowflakeConnection.fetchDDL(info.catalog, info.schema, info.name);
        this.schemaCache.cacheDDL(qualifiedName, ddl);
      } catch (error) {
        ddl = undefined;
      }
    }

    const columnList = table.columns.length > 0
      ? table.columns
          .map((col: any) => `  - \`${col.columnName}\` (${col.dataType})`)
          .join('\n')
      : '  _(Columns not loaded)_';

    const markdown = [
      `### Table: \`${info.name}\``,
      '',
      `**Database**: ${info.catalog}`,
      `**Schema**: ${info.schema}`,
      `**Type**: ${info.kind || info.type}`,
      `**Owner**: ${info.owner}`,
      `**Rows**: ${this.formatNumber(info.rows)}`,
      `**Size**: ${this.formatBytes(info.bytes)}`,
      `**Created**: ${info.created_on}`,
      info.comment ? `**Comment**: ${info.comment}` : '',
      info.cluster_by ? `**Cluster By**: ${info.cluster_by}` : '',
      '',
      `**Columns**: ${table.columns.length}`,
      '',
      '#### Column List:',
      columnList,
    ].filter(line => line !== '').join('\n');

    // Add DDL section if available
    const fullMarkdown = ddl
      ? markdown + '\n\n---\n\n#### DDL:\n```sql\n' + ddl + '\n```'
      : markdown;

    return {
      kind: 'markdown',
      value: fullMarkdown,
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
      column.comment ? `**Comment**: ${column.comment}` : '',
      column.kind && column.kind !== 'COLUMN' ? `**Kind**: ${column.kind}` : '',
      column.autoincrement ? `**Autoincrement**: ${column.autoincrement}` : '',
    ].filter(line => line !== '').join('\n');

    return {
      kind: 'markdown',
      value: markdown,
    };
  }
}
