# Snowflake Language Server

A Language Server Protocol (LSP) implementation for Snowflake SQL, designed for use with Helix editor.

## Features

- **Autocomplete**: Tables, columns, schemas, and SQL keywords
- **Hover**: Display column types and table information
- **Go-to-definition**: Navigate to table/view definitions
- **Semantic checks**: Validate table and column references
- **Formatting**: Integration with sqruff

## Installation

```bash
cd ~/projects/snowflake-lsp
npm install
npm run build
```

## Configuration

Set these environment variables:

```bash
export SNOWFLAKE_ACCOUNT="your_account"
export SNOWFLAKE_USER="your_user"
export SNOWFLAKE_DATABASE="your_database"
export SNOWFLAKE_WAREHOUSE="your_warehouse"
export SNOWFLAKE_ROLE="your_role"  # optional
export SNOWFLAKE_SCHEMA="your_schema"  # optional
```

## Helix Integration

Add to `~/.config/helix/languages.toml`:

```toml
[language-server.snowflake-lsp]
command = "snowflake-lsp"

[[language]]
name = "sql"
language-servers = ["snowflake-lsp"]
```

## Development

```bash
npm run watch    # Watch mode for development
npm run test     # Run tests
npm run build    # Build for production
```

## License

MIT
