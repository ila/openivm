-- {"operators": "INNER_JOIN,AGGREGATE,WINDOW", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM,STOCK", "openivm_verified": true}
SELECT i.I_ID, s.S_W_ID, SUM(s.S_W_ID) OVER (PARTITION BY i.I_ID) AS part_total FROM ITEM i JOIN STOCK s ON i.I_ID = s.S_I_ID;
