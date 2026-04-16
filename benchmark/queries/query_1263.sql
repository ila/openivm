-- {"operators": "AGGREGATE,WINDOW", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER", "openivm_verified": true}
SELECT O_W_ID, O_OL_CNT, SUM(O_OL_CNT) OVER (PARTITION BY O_W_ID) AS part_sum FROM OORDER;
