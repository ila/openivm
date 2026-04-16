-- {"operators": "INNER_JOIN,AGGREGATE,WINDOW", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT", "openivm_verified": true}
SELECT w.W_ID, d.D_ID, SUM(d.D_ID) OVER (PARTITION BY w.W_ID) AS part_total FROM WAREHOUSE w JOIN DISTRICT d ON w.W_ID = d.D_W_ID;
