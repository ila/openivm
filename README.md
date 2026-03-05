# OpenIVM

A DuckDB extension for **Incremental View Maintenance (IVM)**.

OpenIVM allows you to create materialized views in DuckDB and incrementally maintain them when the underlying tables change, without recomputing the entire view from scratch.

## Features

- `CREATE MATERIALIZED VIEW` support (intercepted via parser extension)
- Automatic delta table creation for tracking INSERT/DELETE/UPDATE operations
- Incremental view refresh via `PRAGMA ivm('view_name')`
- Supported query types:
  - Simple projections (`SELECT ... FROM ...`)
  - Filters (`SELECT ... FROM ... WHERE ...`)
  - Aggregations with GROUP BY (`SELECT ..., SUM(...), COUNT(...) FROM ... GROUP BY ...`)
  - Simple aggregations without GROUP BY (`SELECT COUNT(*) FROM ...`)
  - Inner joins

## Usage

```sql
-- Create a materialized view
CREATE MATERIALIZED VIEW product_sales AS
  SELECT product_name, SUM(amount) as total_amount, COUNT(*) as total_orders
  FROM orders GROUP BY product_name;

-- Insert data into the base table
INSERT INTO orders VALUES ('Widget', 100);

-- Refresh the materialized view incrementally
PRAGMA ivm('product_sales');
```

## Building

```sh
make
```

## Testing

```sh
make test
```

## Known Limitations

- `LogicalPlanToString` (lpts) dependency is not yet integrated. The `PRAGMA ivm()` call requires this to convert optimized plans back to SQL. This is tracked as a TODO.
- Only inner joins are supported (no outer/cross joins yet)
- `DROP MATERIALIZED VIEW` is not yet implemented
- Only `SUM` and `COUNT` aggregation functions are supported
