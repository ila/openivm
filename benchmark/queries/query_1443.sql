-- {"operators": "AGGREGATE,WINDOW", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER", "openivm_verified": true}
SELECT O_W_ID, O_D_ID, COUNT(*) AS orders, SUM(COUNT(*)) OVER (PARTITION BY O_W_ID) AS w_total FROM OORDER GROUP BY O_W_ID, O_D_ID;
