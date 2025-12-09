import { Diagnostic, DiagnosticSeverity } from 'vscode-languageserver/node';
import { TextDocument } from 'vscode-languageserver-textdocument';
import { SchemaCache } from './schema-cache';
import { parseContext } from './sql-parser';

export class DiagnosticsProvider {
  constructor(private schemaCache: SchemaCache) {}

  /**
   * Validate document and return diagnostics
   */
  provideDiagnostics(document: TextDocument): Diagnostic[] {
    const diagnostics: Diagnostic[] = [];
    const text = document.getText();

    // Extract table references and validate they exist
    const tableRefs = this.extractTableReferences(text);
    for (const ref of tableRefs) {
      // Try to find the table with multiple strategies
      const exists = this.schemaCache.tableExists(ref.name) ||
                     this.schemaCache.tableExists(ref.name.toUpperCase()) ||
                     this.schemaCache.tableExists(ref.name.toLowerCase());

      if (!exists) {
        // Only show as hint, not error - table might exist but not in cache
        diagnostics.push({
          severity: DiagnosticSeverity.Hint,
          range: ref.range,
          message: `Table '${ref.name}' not found in schema cache (might still be valid)`,
          source: 'snowflake-lsp',
        });
      }
    }

    // Skip column validation for now - it's too complex and error-prone
    // Column validation would require proper SQL parsing to be reliable

    return diagnostics;
  }

  /**
   * Extract table references from SQL text
   */
  private extractTableReferences(text: string): Array<{ name: string; range: any }> {
    const refs: Array<{ name: string; range: any }> = [];
    // Match schema.table or just table, handling optional whitespace around dots
    const pattern = /\b(?:FROM|JOIN)\s+([\w]+(?:\s*\.\s*[\w]+)?)/gi;
    let match;

    while ((match = pattern.exec(text)) !== null) {
      // Remove whitespace around dots (MART. DIM_CURRENCY -> MART.DIM_CURRENCY)
      const tableName = match[1].replace(/\s*\.\s*/g, '.');
      const startIdx = match.index + match[0].indexOf(match[1]);

      refs.push({
        name: tableName,
        range: {
          start: this.offsetToPosition(text, startIdx),
          end: this.offsetToPosition(text, startIdx + match[1].length),
        },
      });
    }

    return refs;
  }

  /**
   * Extract column references from SELECT clause
   */
  private extractColumnReferences(text: string): Array<{ name: string; range: any }> {
    const refs: Array<{ name: string; range: any }> = [];

    // Simple extraction of column names after SELECT
    // This is a basic implementation and could be improved
    const selectPattern = /\bSELECT\s+([\s\S]+?)\s+FROM\b/gi;
    const match = selectPattern.exec(text);

    if (match) {
      const selectList = match[1];
      // Split by comma and extract column names
      const columns = selectList.split(',');

      for (const col of columns) {
        const trimmed = col.trim();
        // Extract column name (ignore aliases and functions)
        const columnMatch = trimmed.match(/^([\w.]+)/);
        if (columnMatch && columnMatch[1] !== '*') {
          const columnName = columnMatch[1].split('.').pop() || columnMatch[1];
          // Note: For simplicity, not calculating exact positions here
          // In a production implementation, you'd want precise ranges
        }
      }
    }

    return refs;
  }

  /**
   * Extract table names from FROM/JOIN clauses
   */
  private extractTablesFromQuery(text: string): string[] {
    const tables: string[] = [];
    const pattern = /\b(?:FROM|JOIN)\s+([\w.]+)/gi;
    let match;

    while ((match = pattern.exec(text)) !== null) {
      tables.push(match[1]);
    }

    return tables;
  }

  /**
   * Convert text offset to LSP Position
   */
  private offsetToPosition(text: string, offset: number): { line: number; character: number } {
    const lines = text.substring(0, offset).split('\n');
    return {
      line: lines.length - 1,
      character: lines[lines.length - 1].length,
    };
  }
}
