-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER", "openivm_verified": true}
SELECT C_W_ID, C_D_ID, COUNT(*) AS cnt FROM CUSTOMER GROUP BY ROLLUP(C_W_ID, C_D_ID);
