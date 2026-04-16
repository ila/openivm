-- {"operators": "FILTER,TABLE_FUNCTION", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE", "openivm_verified": true}
SELECT w.W_ID, s.s AS slot FROM WAREHOUSE w, generate_series(1, 3) s(s) WHERE s.s <= w.W_TAX * 100;
