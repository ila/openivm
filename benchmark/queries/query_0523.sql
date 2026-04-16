-- {"operators": "ORDER,WINDOW", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": true, "tables": "STOCK", "openivm_verified": true}
SELECT S_W_ID, S_I_ID, S_QUANTITY, S_ORDER_CNT, CASE WHEN ROW_NUMBER() OVER (PARTITION BY S_W_ID ORDER BY S_ORDER_CNT DESC) <= 3 THEN 'top3' ELSE 'other' END AS category FROM STOCK;
