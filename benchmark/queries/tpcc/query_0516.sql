-- {"operators": "ORDER,WINDOW", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "STOCK", "openivm_verified": true}
SELECT S_W_ID, S_I_ID, S_QUANTITY, ROW_NUMBER() OVER (PARTITION BY S_W_ID ORDER BY S_QUANTITY ASC) AS qty_rank_asc, ROW_NUMBER() OVER (PARTITION BY S_W_ID ORDER BY S_QUANTITY DESC) AS qty_rank_desc FROM STOCK;
