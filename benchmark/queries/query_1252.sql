-- {"operators": "ORDER,WINDOW", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "STOCK", "openivm_verified": true}
SELECT S_W_ID, S_QUANTITY, LAG(S_QUANTITY, 1) OVER (PARTITION BY S_W_ID ORDER BY S_QUANTITY) AS prev FROM STOCK;
