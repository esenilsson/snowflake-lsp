export enum SQLContext {
  SELECT_LIST,    // After SELECT, before FROM
  FROM_CLAUSE,    // After FROM or JOIN
  WHERE_CLAUSE,   // After WHERE
  TABLE_DOT,      // After table_name.
  SCHEMA_DOT,     // After schema_name.
  GENERAL,        // Default context
}

export interface ParsedContext {
  context: SQLContext;
  currentWord: string;
  tablesInScope: string[];  // Tables mentioned in FROM/JOIN
  previousKeyword: string | null;
}

/**
 * Parse SQL text and determine context at cursor position
 */
export function parseContext(text: string, position: number): ParsedContext {
  const beforeCursor = text.substring(0, position);
  const textLower = beforeCursor.toLowerCase();

  // Extract current word at cursor
  const currentWord = extractCurrentWord(text, position);

  // Find tables in scope (mentioned in FROM/JOIN clauses)
  const tablesInScope = extractTablesInScope(beforeCursor);

  // Determine previous keyword
  const previousKeyword = findPreviousKeyword(beforeCursor);

  // Determine context based on patterns
  let context = SQLContext.GENERAL;

  // Check for schema.| or table.| pattern
  if (/(\w+)\.\s*$/i.test(beforeCursor)) {
    const match = beforeCursor.match(/(\w+)\.\s*$/i);
    if (match) {
      const identifier = match[1].toLowerCase();
      // Simple heuristic: if it's in tablesInScope, it's a table, otherwise assume schema
      if (tablesInScope.some(t => t.toLowerCase().includes(identifier))) {
        context = SQLContext.TABLE_DOT;
      } else {
        context = SQLContext.SCHEMA_DOT;
      }
    }
  }
  // Check for FROM/JOIN context
  else if (/\b(from|join)\s+[^,;\s]*$/i.test(textLower)) {
    context = SQLContext.FROM_CLAUSE;
  }
  // Check for WHERE context
  else if (/\bwhere\b/i.test(textLower) &&
           !/(group\s+by|order\s+by|having)/i.test(textLower.substring(textLower.lastIndexOf('where')))) {
    context = SQLContext.WHERE_CLAUSE;
  }
  // Check for SELECT context (between SELECT and FROM)
  else if (/\bselect\b/i.test(textLower) && !/\bfrom\b/i.test(textLower)) {
    context = SQLContext.SELECT_LIST;
  }

  return {
    context,
    currentWord,
    tablesInScope,
    previousKeyword,
  };
}

/**
 * Extract the current word at cursor position
 */
function extractCurrentWord(text: string, position: number): string {
  const before = text.substring(0, position);
  const after = text.substring(position);

  // Find word boundary before cursor
  const beforeMatch = before.match(/[\w.]*$/);
  const beforePart = beforeMatch ? beforeMatch[0] : '';

  // Find word boundary after cursor
  const afterMatch = after.match(/^[\w.]*/);
  const afterPart = afterMatch ? afterMatch[0] : '';

  return beforePart + afterPart;
}

/**
 * Find the previous SQL keyword before cursor
 */
function findPreviousKeyword(text: string): string | null {
  const keywords = ['select', 'from', 'where', 'join', 'inner', 'left', 'right',
                    'outer', 'on', 'group', 'by', 'order', 'having', 'as', 'and', 'or'];

  const words = text.toLowerCase().split(/\s+/);

  for (let i = words.length - 1; i >= 0; i--) {
    if (keywords.includes(words[i])) {
      return words[i];
    }
  }

  return null;
}

/**
 * Extract table names from FROM and JOIN clauses
 */
function extractTablesInScope(text: string): string[] {
  const tables: string[] = [];
  const textLower = text.toLowerCase();

  // Match FROM table_name [AS alias]
  const fromPattern = /\bfrom\s+([\w.]+)(?:\s+as\s+(\w+))?/gi;
  let match;
  while ((match = fromPattern.exec(textLower)) !== null) {
    tables.push(match[1]);  // table name
    if (match[2]) {
      tables.push(match[2]);  // alias
    }
  }

  // Match JOIN table_name [AS alias]
  const joinPattern = /\bjoin\s+([\w.]+)(?:\s+as\s+(\w+))?/gi;
  while ((match = joinPattern.exec(textLower)) !== null) {
    tables.push(match[1]);  // table name
    if (match[2]) {
      tables.push(match[2]);  // alias
    }
  }

  return tables;
}

/**
 * Get SQL keywords for autocomplete
 */
export function getSQLKeywords(): string[] {
  return [
    'SELECT', 'FROM', 'WHERE', 'JOIN', 'INNER', 'LEFT', 'RIGHT', 'OUTER',
    'ON', 'AS', 'AND', 'OR', 'NOT', 'IN', 'EXISTS', 'BETWEEN', 'LIKE',
    'GROUP', 'BY', 'ORDER', 'HAVING', 'LIMIT', 'OFFSET',
    'INSERT', 'INTO', 'VALUES', 'UPDATE', 'SET', 'DELETE',
    'CREATE', 'TABLE', 'VIEW', 'DROP', 'ALTER', 'ADD', 'COLUMN',
    'DISTINCT', 'COUNT', 'SUM', 'AVG', 'MIN', 'MAX',
    'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
    'CAST', 'COALESCE', 'NULLIF', 'IS', 'NULL',
    'WITH', 'UNION', 'ALL', 'INTERSECT', 'EXCEPT',
  ];
}

/**
 * Check if a word looks like a SQL keyword
 */
export function isLikelyKeyword(word: string): boolean {
  const keywords = getSQLKeywords().map(k => k.toLowerCase());
  return keywords.includes(word.toLowerCase());
}
