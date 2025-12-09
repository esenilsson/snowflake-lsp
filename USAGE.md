# Snowflake Language Server - Usage Guide

The Snowflake Language Server has been successfully installed and configured!

## Setup Complete ✓

The following has been done:
- ✓ Language server implemented with TypeScript
- ✓ All modules created (completion, hover, diagnostics, formatting)
- ✓ Project built and linked globally
- ✓ Helix configuration updated

## Configuration Required

Before using the language server, you **must** set these environment variables:

```bash
export SNOWFLAKE_ACCOUNT="your_account"
export SNOWFLAKE_USER="your_user"
export SNOWFLAKE_DATABASE="your_database"
export SNOWFLAKE_WAREHOUSE="your_warehouse"
export SNOWFLAKE_ROLE="your_role"          # optional
export SNOWFLAKE_SCHEMA="your_schema"      # optional
```

Add these to your `~/.zshrc` or `~/.bashrc` to make them permanent.

## How to Test

### 1. Set Environment Variables

```bash
export SNOWFLAKE_ACCOUNT="myaccount"
export SNOWFLAKE_USER="myuser"
export SNOWFLAKE_DATABASE="mydatabase"
export SNOWFLAKE_WAREHOUSE="mywarehouse"
```

### 2. Create a Test SQL File

```bash
echo "SELECT * FROM " > ~/test.sql
```

### 3. Open in Helix

```bash
hx ~/test.sql
```

### 4. Wait for Connection

When Helix starts:
- The language server will connect to Snowflake
- Your browser will open for SSO authentication (`externalbrowser`)
- After authentication, the language server will load your schema

Watch the Helix logs with `:log-open` to see progress.

### 5. Test Features

**Autocomplete:**
- Type `SELECT * FROM ` and wait - you should see table suggestions
- Type `SELECT ` and then a column name - you should see column suggestions
- Type a schema name followed by `.` - you should see tables in that schema

**Hover:**
- Hover over a table name (press `Space + k`) - see table info
- Hover over a column name - see column type and details

**Formatting:**
- Select some SQL and run `:format` - it will format using sqruff

**Diagnostics:**
- Reference a non-existent table - you should see an error
- Reference a non-existent column - you should see a warning

## Troubleshooting

### Language Server Not Starting

1. Check environment variables are set:
   ```bash
   echo $SNOWFLAKE_ACCOUNT
   ```

2. Check the language server is in PATH:
   ```bash
   which snowflake-lsp
   ```

3. View Helix logs:
   ```bash
   # In Helix, type:
   :log-open
   ```

### Connection Fails

- Ensure your Snowflake credentials are correct
- Make sure SSO browser authentication is enabled for your account
- Check network connectivity to Snowflake

### No Completions Showing

- Wait a few seconds after opening - schema loading takes time
- Check `:log-open` to see if schema was loaded successfully
- Verify the language server is running: `:lsp-workspace-command`

## Features

### Implemented ✓
- Autocomplete for tables, columns, schemas, SQL keywords
- Context-aware completions (different suggestions after FROM vs SELECT)
- Hover information showing data types and table details
- Semantic validation (invalid table/column references)
- Formatting via sqruff integration

### Not Yet Implemented (Future)
- sqlfluff linting (deferred to v2)
- dbt model integration
- Query execution from editor
- Multiple connection profiles

## Development

To make changes to the language server:

```bash
cd ~/projects/snowflake-lsp
npm run watch       # Auto-rebuild on changes
npm run test        # Run tests (when implemented)
```

After making changes, restart Helix to pick up the new version.

## Project Structure

```
~/projects/snowflake-lsp/
├── src/
│   ├── server.ts          # Main LSP server
│   ├── snowflake.ts       # Snowflake connection
│   ├── schema-cache.ts    # Schema caching
│   ├── completion.ts      # Autocomplete
│   ├── hover.ts           # Hover info
│   ├── definition.ts      # Go-to-definition
│   ├── diagnostics.ts     # Error checking
│   ├── formatting.ts      # Code formatting
│   └── sql-parser.ts      # SQL context detection
├── dist/                  # Compiled JS (generated)
└── bin/
    └── snowflake-lsp      # Executable entry point
```

## Helix Configuration

The language server is configured in `~/.config/helix/languages.toml`:

```toml
[language-server.snowflake-lsp]
command = "snowflake-lsp"

[[language]]
name = "sql"
language-servers = ["snowflake-lsp", "sqruff"]
```

Both language servers work together:
- `snowflake-lsp` provides Snowflake-specific features
- `sqruff` provides SQL linting and formatting

## Support

For issues or questions:
1. Check the logs in Helix (`:log-open`)
2. Review the plan file: `~/.claude/plans/polymorphic-juggling-eagle.md`
3. Check the project README: `~/projects/snowflake-lsp/README.md`
