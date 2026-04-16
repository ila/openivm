-- {"operators": "CROSS_JOIN,FILTER,TABLE_FUNCTION", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE", "openivm_verified": true}
SELECT W_ID, W_NAME, s.i FROM WAREHOUSE CROSS JOIN generate_series(1, 5) s(i) WHERE W_ID = s.i;
