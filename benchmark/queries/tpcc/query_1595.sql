-- {"operators": "TABLE_FUNCTION,VALUES_ONLY", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": true, "has_case": false, "tables": "UNKNOWN", "openivm_verified": true}
SELECT range AS n, n::VARCHAR AS n_str FROM range(100, 110);
