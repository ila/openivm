-- {"operators": "INNER_JOIN,ORDER,WINDOW", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,OORDER", "openivm_verified": true}
SELECT w.W_ID, o.O_OL_CNT, ROW_NUMBER() OVER (PARTITION BY w.W_ID ORDER BY o.O_OL_CNT DESC) AS rn FROM WAREHOUSE w JOIN OORDER o ON w.W_ID = o.O_W_ID;
