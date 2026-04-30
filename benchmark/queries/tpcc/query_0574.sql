-- {"operators": "OUTER_JOIN,AGGREGATE,TABLE_FUNCTION", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT s AS warehouse_id, COUNT(c.C_ID) AS customer_count FROM generate_series(1, 5) t(s) LEFT JOIN CUSTOMER c ON c.C_W_ID = t.s GROUP BY s;
