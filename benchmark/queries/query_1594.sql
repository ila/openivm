-- {"operators": "TABLE_FUNCTION,VALUES_ONLY", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "UNKNOWN", "openivm_verified": true}
SELECT generate_series AS n, n * n AS n_squared FROM generate_series(1, 10);
