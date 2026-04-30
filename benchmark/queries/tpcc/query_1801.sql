-- {"operators": "INNER_JOIN,ORDER,WINDOW", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM,STOCK", "openivm_verified": true}
SELECT i.I_ID, s.S_W_ID, ROW_NUMBER() OVER (PARTITION BY i.I_ID ORDER BY s.S_W_ID DESC) AS rn FROM ITEM i JOIN STOCK s ON i.I_ID = s.S_I_ID;
