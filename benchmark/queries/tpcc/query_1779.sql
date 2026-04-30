-- {"operators": "INNER_JOIN,AGGREGATE,WINDOW", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "DISTRICT,OORDER", "openivm_verified": true}
SELECT d.D_W_ID, o.O_OL_CNT, SUM(o.O_OL_CNT) OVER (PARTITION BY d.D_W_ID) AS part_total FROM DISTRICT d JOIN OORDER o ON d.D_W_ID = o.O_W_ID AND d.D_ID = o.O_D_ID;
