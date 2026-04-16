-- {"operators": "AGGREGATE,FILTER,ORDER,HAVING", "complexity": "medium", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER", "non_incr_reason": "op:ORDER"}
SELECT O_W_ID, O_D_ID, COUNT(*) as order_cnt FROM OORDER WHERE O_ALL_LOCAL = 0 GROUP BY O_W_ID, O_D_ID HAVING COUNT(*) >= 5 ORDER BY O_W_ID, O_D_ID;
