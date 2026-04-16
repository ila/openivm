-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "STOCK", "openivm_verified": true}
SELECT S_W_ID, COUNT(*) AS items, SUM(S_QUANTITY) AS stock FROM STOCK GROUP BY ROLLUP(S_W_ID);
