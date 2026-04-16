-- {"operators": "FILTER,ORDER", "complexity": "low", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER", "non_incr_reason": "op:ORDER"}
SELECT * FROM OORDER WHERE O_W_ID > 0 ORDER BY O_W_ID;
