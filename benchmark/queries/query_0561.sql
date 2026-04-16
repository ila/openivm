-- {"operators": "TABLE_FUNCTION", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE", "openivm_verified": true}
SELECT W_ID, r.n AS slot FROM WAREHOUSE, range(1, 11) r(n);
