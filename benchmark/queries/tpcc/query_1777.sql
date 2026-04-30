-- {"operators": "INNER_JOIN,ORDER,WINDOW", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "DISTRICT,OORDER", "openivm_verified": true}
SELECT d.D_W_ID, o.O_OL_CNT, ROW_NUMBER() OVER (PARTITION BY d.D_W_ID ORDER BY o.O_OL_CNT DESC) AS rn FROM DISTRICT d JOIN OORDER o ON d.D_W_ID = o.O_W_ID AND d.D_ID = o.O_D_ID;
