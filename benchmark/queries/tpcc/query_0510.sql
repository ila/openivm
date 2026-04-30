-- {"operators": "ORDER,WINDOW", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "STOCK", "openivm_verified": true}
SELECT S_W_ID, S_I_ID, S_QUANTITY, LAST_VALUE(S_QUANTITY) OVER (PARTITION BY S_W_ID ORDER BY S_QUANTITY ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_in_wh FROM STOCK;
