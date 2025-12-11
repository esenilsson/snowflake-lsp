# Snowflake LSP Feature Enhancement Tasks

This document outlines essential API and SQL commands to match core features of the official Snowflake VS Code extension in the `snowflake-lsp` project.

## Schema and Table Introspection

- List all databases:  
  `SHOW DATABASES;`
- List schemas in a database:  
  `SHOW SCHEMAS IN DATABASE <db_name>;`
- List tables in a schema:  
  `SHOW TABLES IN SCHEMA <db_name>.<schema_name>;`
- Get table/column details:  
  `DESCRIBE TABLE <db_name>.<schema_name>.<table_name>;`  
  or  
  `SELECT COLUMN_NAME, DATA_TYPE, COMMENT FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '<schema>' AND TABLE_NAME = '<table>';`

## Session Context Awareness

- Get current session context:  
  `SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE(), CURRENT_ROLE();`
- Switch database/schema:  
  `USE DATABASE <db_name>;`  
  `USE SCHEMA <schema_name>;`
- Switch warehouse:  
  `USE WAREHOUSE <warehouse_name>;`
- Switch role:  
  `USE ROLE <role_name>;`

## Table/Column Metadata for Hover and Go-to-Definition

- Fetch column types and comments:  
  `SELECT COLUMN_NAME, DATA_TYPE, COMMENT FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '<schema>' AND TABLE_NAME = '<table>';`
- Fetch table definition (DDL):  
  `SHOW CREATE TABLE <db_name>.<schema_name>.<table_name>;`

## Semantic Validation

- Validate table existence:  
  `SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '<schema>' AND TABLE_NAME = '<table>';`
- Validate column existence:  
  `SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '<schema>' AND TABLE_NAME = '<table>' AND COLUMN_NAME = '<column>';`

## Query History and Recent Activity

- Fetch recent queries:  
  `SELECT * FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY()) ORDER BY START_TIME DESC LIMIT 10;`

## REST API Integration

- Authenticate with Snowflake via OAuth or key pair.
- Use Snowflake’s REST API for:
  - Database/schema/table listing.
  - Session context.
  - Query history.
  - Table/column metadata.

## Formatting and Linting

- Integrate with `sqlfluff` or similar for SQL formatting.
- Add basic linting for Snowflake SQL.

## Advanced Features (Optional)

- List warehouses:  
  `SHOW WAREHOUSES;`
- List roles:  
  `SHOW ROLES;`
- List users:  
  `SHOW USERS;`
- Query warehouse status:  
  `SELECT * FROM INFORMATION_SCHEMA.WAREHOUSE_LOAD_HISTORY;`
- Query role grants:  
  `SHOW GRANTS TO ROLE <role_name>;`
