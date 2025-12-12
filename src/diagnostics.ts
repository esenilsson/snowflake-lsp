import { Diagnostic, DiagnosticSeverity } from 'vscode-languageserver/node';
import { TextDocument } from 'vscode-languageserver-textdocument';
import { SchemaCache } from './schema-cache';
import { parseContext } from './sql-parser';
import { spawn } from 'child_process';

export class DiagnosticsProvider {
  private sqlfluffEnabled: boolean;

  constructor(private schemaCache: SchemaCache) {
    // Check if sqlfluff is enabled via environment variable
    this.sqlfluffEnabled = process.env.SNOWFLAKE_LSP_ENABLE_SQLFLUFF === 'true';
  }

  /**
   * Validate document and return diagnostics
   */
  async provideDiagnostics(document: TextDocument): Promise<Diagnostic[]> {
    const diagnostics: Diagnostic[] = [];
    const text = document.getText();

    // Extract table references and validate they exist (semantic checks)
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

    // Run sqlfluff linting if enabled
    if (this.sqlfluffEnabled) {
      try {
        const sqlfluffDiagnostics = await this.runSqlfluff(text);
        diagnostics.push(...sqlfluffDiagnostics);
      } catch (error) {
        // Silently fail - sqlfluff might not be installed
        console.log('sqlfluff not available or failed:', error);
      }
    }

    return diagnostics;
  }

  /**
   * Run sqlfluff and parse output
   */
  private async runSqlfluff(text: string): Promise<Diagnostic[]> {
    return new Promise((resolve) => {
      try {
        const process = spawn('sqlfluff', ['lint', '--dialect', 'snowflake', '--format', 'json', '-']);

        let stdout = '';
        let stderr = '';

        process.stdout.on('data', (data) => {
          stdout += data.toString();
        });

        process.stderr.on('data', (data) => {
          stderr += data.toString();
        });

        process.on('error', (error: any) => {
          // Command not found or other spawn error
          if (error.code === 'ENOENT') {
            console.log('sqlfluff command not found - install with: pip install sqlfluff');
          } else {
            console.log('sqlfluff error:', error);
          }
          resolve([]);
        });

        process.on('close', (code) => {
          try {
            // sqlfluff returns non-zero exit code when violations are found
            // So we don't treat non-zero as an error
            if (stdout) {
              const result = JSON.parse(stdout);
              resolve(this.parseSqlfluffOutput(result));
            } else {
              resolve([]);
            }
          } catch (error) {
            console.log('Failed to parse sqlfluff output:', error);
            resolve([]);
          }
        });

        // Set timeout
        const timeout = setTimeout(() => {
          process.kill();
          resolve([]);
        }, 5000);

        process.on('close', () => {
          clearTimeout(timeout);
        });

        // Write SQL to stdin
        process.stdin.write(text);
        process.stdin.end();
      } catch (error) {
        console.log('sqlfluff spawn error:', error);
        resolve([]);
      }
    });
  }

  /**
   * Parse sqlfluff JSON output to LSP diagnostics
   */
  private parseSqlfluffOutput(result: any): Diagnostic[] {
    const diagnostics: Diagnostic[] = [];

    if (!result || !Array.isArray(result)) {
      return diagnostics;
    }

    // sqlfluff returns array of file results
    for (const fileResult of result) {
      if (!fileResult.violations || !Array.isArray(fileResult.violations)) {
        continue;
      }

      for (const violation of fileResult.violations) {
        // Map sqlfluff severity to LSP severity
        let severity: DiagnosticSeverity;
        // sqlfluff doesn't have explicit severity, so we treat all as warnings
        severity = DiagnosticSeverity.Warning;

        const diagnostic: Diagnostic = {
          severity,
          range: {
            start: {
              line: (violation.line_no || 1) - 1, // LSP uses 0-based lines
              character: (violation.line_pos || 1) - 1, // LSP uses 0-based characters
            },
            end: {
              line: (violation.line_no || 1) - 1,
              character: (violation.line_pos || 1) + 10, // Estimate end position
            },
          },
          message: `[${violation.code}] ${violation.description}`,
          source: 'sqlfluff',
          code: violation.code,
        };

        diagnostics.push(diagnostic);
      }
    }

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
