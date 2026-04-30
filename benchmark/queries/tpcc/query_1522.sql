-- {"operators": "INNER_JOIN,ORDER,WINDOW", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM,STOCK", "openivm_verified": true}
SELECT i.I_ID, i.I_NAME, i.I_PRICE, s.S_W_ID, s.S_QUANTITY, DENSE_RANK() OVER (PARTITION BY s.S_W_ID ORDER BY s.S_QUANTITY DESC) AS stock_rank FROM ITEM i JOIN STOCK s ON i.I_ID = s.S_I_ID;
