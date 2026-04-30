-- {"operators": "TABLE_FUNCTION,VALUES_ONLY", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "UNKNOWN", "openivm_verified": true}
SELECT n FROM range(1, 20) t(n);
