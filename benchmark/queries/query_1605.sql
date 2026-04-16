-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "STOCK"}
SELECT S_W_ID, S_I_ID, S_QUANTITY, IF(S_QUANTITY > 50, S_QUANTITY, NULL) AS high_only FROM STOCK;
