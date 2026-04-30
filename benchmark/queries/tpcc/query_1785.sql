-- {"operators": "INNER_JOIN,AGGREGATE,WINDOW", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER,OORDER", "openivm_verified": true}
SELECT c.C_ID, o.O_OL_CNT, SUM(o.O_OL_CNT) OVER (PARTITION BY c.C_ID) AS part_total FROM CUSTOMER c JOIN OORDER o ON c.C_ID = o.O_C_ID AND c.C_W_ID = o.O_W_ID;
