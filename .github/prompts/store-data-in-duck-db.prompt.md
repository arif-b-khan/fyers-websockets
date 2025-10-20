---
mode: agent
---

Execute DuckDB database operations to:

1. Create and configure a DuckDB connection
2. Insert provided data into appropriate tables with proper schema
3. Ensure data persistence across sessions
4. Implement query functionality to retrieve stored data
5. Handle errors gracefully and provide feedback
6. Close database connections properly

Technical Requirements:
- Use DuckDB Python API
- Define table schemas before insertion
- Support common data formats (CSV, JSON, Parquet)
- Implement CRUD operations
- Maintain data integrity
- Use parameterized queries for security

Expected Output:
- Confirmation of successful data operations
- Query results in a specified format
- Error messages for failed operations

Documentation Reference:
- DuckDB Python API: https://duckdb.org/docs/api/python/overview
- DuckDB SQL Reference: https://duckdb.org/docs/sql/introduction