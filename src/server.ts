import {
  createConnection,
  TextDocuments,
  ProposedFeatures,
  InitializeParams,
  InitializeResult,
  TextDocumentSyncKind,
  CompletionItem,
} from 'vscode-languageserver/node';
import { TextDocument } from 'vscode-languageserver-textdocument';
import { SnowflakeConnection, loadConfigFromEnv } from './snowflake';
import { SchemaCache } from './schema-cache';
import { CompletionProvider } from './completion';
import { HoverProvider } from './hover';
import { DefinitionProvider } from './definition';
import { DiagnosticsProvider } from './diagnostics';
import { FormattingProvider } from './formatting';
import { SessionContextManager } from './session-context';
import { parseUseCommands } from './sql-parser';

// Create LSP connection using stdio for communication with Helix
const connection = createConnection(ProposedFeatures.all, process.stdin, process.stdout);

// Create document manager
const documents = new TextDocuments(TextDocument);

// Initialize providers
let snowflakeConnection: SnowflakeConnection;
let schemaCache: SchemaCache;
let completionProvider: CompletionProvider;
let hoverProvider: HoverProvider;
let definitionProvider: DefinitionProvider;
let diagnosticsProvider: DiagnosticsProvider;
let formattingProvider: FormattingProvider;
let sessionContextManager: SessionContextManager;

let isInitialized = false;

/**
 * Initialize the language server
 */
connection.onInitialize(async (params: InitializeParams) => {
  connection.console.log('Initializing Snowflake Language Server...');

  const result: InitializeResult = {
    capabilities: {
      textDocumentSync: TextDocumentSyncKind.Incremental,
      completionProvider: {
        resolveProvider: false,
        triggerCharacters: ['.', ' '],
      },
      hoverProvider: true,
      definitionProvider: true,
      documentFormattingProvider: true,
    },
  };

  return result;
});

/**
 * After initialization, connect to Snowflake and load schema
 */
connection.onInitialized(async () => {
  try {
    connection.console.log('Loading Snowflake configuration...');

    // Load configuration from environment
    const config = loadConfigFromEnv();
    connection.console.log(`Connecting to Snowflake account: ${config.account}`);

    // Initialize Snowflake connection
    snowflakeConnection = new SnowflakeConnection(config);
    await snowflakeConnection.connect();

    connection.console.log('Connected to Snowflake successfully');

    // Initialize schema cache
    schemaCache = new SchemaCache();

    connection.console.log('Loading schema from Snowflake...');

    // Load schema data (columns loaded lazily on-demand)
    const [tables, views, warehouses, roles, users, databases, queryHistory] = await Promise.all([
      snowflakeConnection.fetchTables(),
      snowflakeConnection.fetchViews(),
      snowflakeConnection.fetchWarehouses(),
      snowflakeConnection.fetchRoles(),
      snowflakeConnection.fetchUsers(),
      snowflakeConnection.fetchDatabases(),
      snowflakeConnection.fetchQueryHistory(),
    ]);

    connection.console.log(`Loaded ${tables.length} tables, ${views.length} views`);
    connection.console.log(`Loaded ${warehouses.length} warehouses, ${roles.length} roles, ${users.length} users, ${databases.length} databases`);
    connection.console.log(`Loaded ${queryHistory.length} queries from history`);
    connection.console.log('Note: Columns will be loaded lazily on first access');

    // Load tables without columns (empty array) - columns loaded on-demand
    schemaCache.loadTables(tables, []);
    schemaCache.loadViews(views);
    schemaCache.loadWarehouses(warehouses);
    schemaCache.loadRoles(roles);
    schemaCache.loadUsers(users);
    schemaCache.loadDatabases(databases);
    schemaCache.loadQueryHistory(queryHistory);

    const stats = schemaCache.getStats();
    connection.console.log(`Schema cache populated: ${JSON.stringify(stats)}`);

    // Initialize providers
    completionProvider = new CompletionProvider(schemaCache);
    hoverProvider = new HoverProvider(schemaCache, snowflakeConnection);
    definitionProvider = new DefinitionProvider(schemaCache);
    diagnosticsProvider = new DiagnosticsProvider(schemaCache);
    formattingProvider = new FormattingProvider();

    // Initialize session context manager
    sessionContextManager = new SessionContextManager();
    await sessionContextManager.initializeGlobalContext(snowflakeConnection);
    connection.console.log('Session context initialized');

    isInitialized = true;

    connection.console.log('Snowflake Language Server initialized successfully');
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    connection.console.error(`Initialization error: ${errorMessage}`);
    // Note: Helix doesn't support window.showErrorMessage, errors are shown in logs only
  }
});

/**
 * Handle completion requests
 */
connection.onCompletion(async (params): Promise<CompletionItem[]> => {
  if (!isInitialized || !completionProvider) {
    return [];
  }

  const document = documents.get(params.textDocument.uri);
  if (!document) {
    return [];
  }

  try {
    // Get session context for this document
    const context = sessionContextManager?.getContext(params.textDocument.uri);
    return completionProvider.provideCompletions(document, params, context);
  } catch (error) {
    connection.console.error(`Completion error: ${error}`);
    return [];
  }
});

/**
 * Handle hover requests
 */
connection.onHover(async (params) => {
  if (!isInitialized || !hoverProvider) {
    return undefined;
  }

  const document = documents.get(params.textDocument.uri);
  if (!document) {
    return undefined;
  }

  try {
    return await hoverProvider.provideHover(document, params);
  } catch (error) {
    connection.console.error(`Hover error: ${error}`);
    return undefined;
  }
});

/**
 * Handle definition requests
 */
connection.onDefinition(async (params) => {
  if (!isInitialized || !definitionProvider) {
    return undefined;
  }

  const document = documents.get(params.textDocument.uri);
  if (!document) {
    return undefined;
  }

  try {
    return definitionProvider.provideDefinition(document, params);
  } catch (error) {
    connection.console.error(`Definition error: ${error}`);
    return undefined;
  }
});

/**
 * Handle document formatting requests
 */
connection.onDocumentFormatting(async (params) => {
  if (!isInitialized || !formattingProvider) {
    return [];
  }

  const document = documents.get(params.textDocument.uri);
  if (!document) {
    return [];
  }

  try {
    return await formattingProvider.formatDocument(document);
  } catch (error) {
    connection.console.error(`Formatting error: ${error}`);
    return [];
  }
});

/**
 * Validate document when it's opened or changed
 */
async function validateDocument(document: TextDocument): Promise<void> {
  if (!isInitialized || !diagnosticsProvider) {
    return;
  }

  try {
    const diagnostics = await diagnosticsProvider.provideDiagnostics(document);
    connection.sendDiagnostics({
      uri: document.uri,
      diagnostics,
    });
  } catch (error) {
    connection.console.error(`Validation error: ${error}`);
  }
}

// Document change handlers
documents.onDidOpen((event) => {
  // Parse USE commands and initialize context for this document
  const useCommands = parseUseCommands(event.document.getText());
  if (sessionContextManager) {
    sessionContextManager.updateContext(event.document.uri, useCommands);
  }
  validateDocument(event.document);
});

documents.onDidChangeContent((change) => {
  // Re-parse USE commands on document change
  // TODO: Optimize by only parsing when text contains "USE"
  if (sessionContextManager) {
    const useCommands = parseUseCommands(change.document.getText());
    sessionContextManager.updateContext(change.document.uri, useCommands);
  }
  validateDocument(change.document);
});

documents.onDidClose((event) => {
  // Clear session context for this document
  if (sessionContextManager) {
    sessionContextManager.clearContext(event.document.uri);
  }
  // Clear diagnostics when document is closed
  connection.sendDiagnostics({
    uri: event.document.uri,
    diagnostics: [],
  });
});

// Make documents listen to the connection
documents.listen(connection);

// Listen on the connection
connection.listen();

// Handle cleanup on shutdown
process.on('SIGINT', async () => {
  connection.console.log('Shutting down Snowflake Language Server...');
  if (snowflakeConnection) {
    await snowflakeConnection.disconnect();
  }
  process.exit(0);
});
