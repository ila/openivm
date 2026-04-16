-- {"operators": "INNER_JOIN,ORDER,WINDOW", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM,STOCK", "openivm_verified": true}
SELECT i.I_ID, s.S_W_ID, DENSE_RANK() OVER (PARTITION BY i.I_ID ORDER BY s.S_W_ID DESC) AS dr, NTILE(4) OVER (PARTITION BY i.I_ID ORDER BY s.S_W_ID) AS q FROM ITEM i JOIN STOCK s ON i.I_ID = s.S_I_ID;
