import { Definition, Location, TextDocumentPositionParams } from 'vscode-languageserver/node';
import { TextDocument } from 'vscode-languageserver-textdocument';
import { SchemaCache } from './schema-cache';
import { parseContext } from './sql-parser';

export class DefinitionProvider {
  constructor(private schemaCache: SchemaCache) {}

  /**
   * Provide definition location for symbol at cursor
   * Note: For schema metadata (tables/columns), we don't have actual file locations,
   * so this will return undefined. The hover provider shows the information instead.
   */
  provideDefinition(
    document: TextDocument,
    params: TextDocumentPositionParams
  ): Definition | undefined {
    const text = document.getText();
    const offset = document.offsetAt(params.position);

    // Parse context to get current word
    const parsed = parseContext(text, offset);
    const word = parsed.currentWord;

    if (!word) return undefined;

    // Check if it's a table
    const table = this.schemaCache.getTable(word);
    if (table) {
      // For tables, we don't have file locations (they're in Snowflake)
      // Return undefined - hover provider will show table info
      return undefined;
    }

    // Check if it's a column
    for (const tableName of parsed.tablesInScope) {
      const columns = this.schemaCache.getTableColumns(tableName);
      const column = columns.find(c =>
        c.columnName.toLowerCase() === word.toLowerCase()
      );

      if (column) {
        // For columns, we don't have file locations
        // Return undefined - hover provider will show column info
        return undefined;
      }
    }

    return undefined;
  }
}
