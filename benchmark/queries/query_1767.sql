-- {"operators": "INNER_JOIN,AGGREGATE,WINDOW", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,OORDER", "openivm_verified": true}
SELECT w.W_ID, o.O_OL_CNT, SUM(o.O_OL_CNT) OVER (PARTITION BY w.W_ID) AS part_total FROM WAREHOUSE w JOIN OORDER o ON w.W_ID = o.O_W_ID;
