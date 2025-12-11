import { UseCommand, UseCommandType } from './session-context';

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
  aliases: Map<string, string>;  // alias -> table name mapping
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

  // Find tables in scope (mentioned in FROM/JOIN clauses) and their aliases
  const { tables: tablesInScope, aliases } = extractTablesAndAliases(beforeCursor);

  // Determine previous keyword
  const previousKeyword = findPreviousKeyword(beforeCursor);

  // Determine context based on patterns
  let context = SQLContext.GENERAL;

  // Check for schema.| or table.| or alias.| pattern
  if (/(\w+)\.\s*$/i.test(beforeCursor)) {
    const match = beforeCursor.match(/(\w+)\.\s*$/i);
    if (match) {
      const identifier = match[1].toLowerCase();
      // Check if it's an alias first, then table, otherwise assume schema
      if (aliases.has(identifier)) {
        context = SQLContext.TABLE_DOT;
      } else if (tablesInScope.some(t => t.toLowerCase().includes(identifier))) {
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
    aliases,
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
 * Extract table names and aliases from FROM and JOIN clauses
 */
function extractTablesAndAliases(text: string): { tables: string[]; aliases: Map<string, string> } {
  const tables: string[] = [];
  const aliases = new Map<string, string>();
  const textLower = text.toLowerCase();

  // Match FROM table_name [AS] alias or FROM table_name
  // Handles: FROM table, FROM table alias, FROM table AS alias
  const fromPattern = /\bfrom\s+([\w.]+)(?:\s+(?:as\s+)?(\w+))?/gi;
  let match;
  while ((match = fromPattern.exec(textLower)) !== null) {
    const tableName = match[1];
    const alias = match[2];

    tables.push(tableName);

    if (alias && alias !== 'where' && alias !== 'join' && alias !== 'left' && alias !== 'right' && alias !== 'inner' && alias !== 'outer') {
      aliases.set(alias, tableName);
      tables.push(alias);
    }
  }

  // Match JOIN table_name [AS] alias or JOIN table_name
  const joinPattern = /\bjoin\s+([\w.]+)(?:\s+(?:as\s+)?(\w+))?/gi;
  while ((match = joinPattern.exec(textLower)) !== null) {
    const tableName = match[1];
    const alias = match[2];

    tables.push(tableName);

    if (alias && alias !== 'on' && alias !== 'where' && alias !== 'join' && alias !== 'left' && alias !== 'right' && alias !== 'inner' && alias !== 'outer') {
      aliases.set(alias, tableName);
      tables.push(alias);
    }
  }

  return { tables, aliases };
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

/**
 * Parse USE commands from SQL text
 * Detects: USE DATABASE, USE SCHEMA, USE WAREHOUSE, USE ROLE
 * Also handles shorthand: USE <name> → USE DATABASE <name>
 * And qualified: USE SCHEMA db.schema → USE DATABASE db + USE SCHEMA schema
 */
export function parseUseCommands(text: string): UseCommand[] {
  const commands: UseCommand[] = [];
  const lines = text.split('\n');

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];

    // Skip single-line comments
    if (/^\s*--/.test(line)) continue;

    // Skip multi-line comments (simple check)
    if (/^\s*\/\*/.test(line)) continue;

    // Match USE patterns (case-insensitive)
    // Order matters: check specific patterns first, then shorthand

    // USE DATABASE <name>
    const useDb = line.match(/USE\s+DATABASE\s+(\w+)/i);
    if (useDb) {
      commands.push({ type: UseCommandType.DATABASE, value: useDb[1], line: i });
      continue;
    }

    // USE SCHEMA <name> or USE SCHEMA <db>.<schema>
    const useSchema = line.match(/USE\s+SCHEMA\s+([\w.]+)/i);
    if (useSchema) {
      // Handle qualified: PROD.ANALYTICS → DATABASE=PROD, SCHEMA=ANALYTICS
      const parts = useSchema[1].split('.');
      if (parts.length === 2) {
        commands.push({ type: UseCommandType.DATABASE, value: parts[0], line: i });
        commands.push({ type: UseCommandType.SCHEMA, value: parts[1], line: i });
      } else {
        commands.push({ type: UseCommandType.SCHEMA, value: useSchema[1], line: i });
      }
      continue;
    }

    // USE WAREHOUSE <name>
    const useWh = line.match(/USE\s+WAREHOUSE\s+(\w+)/i);
    if (useWh) {
      commands.push({ type: UseCommandType.WAREHOUSE, value: useWh[1], line: i });
      continue;
    }

    // USE ROLE <name>
    const useRole = line.match(/USE\s+ROLE\s+(\w+)/i);
    if (useRole) {
      commands.push({ type: UseCommandType.ROLE, value: useRole[1], line: i });
      continue;
    }

    // Shorthand: USE <name> → USE DATABASE <name>
    // Only match if none of the specific patterns matched
    const useShort = line.match(/USE\s+(\w+)/i);
    if (useShort) {
      commands.push({ type: UseCommandType.DATABASE, value: useShort[1], line: i });
    }
  }

  return commands;
}
