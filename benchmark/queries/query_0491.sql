-- {"operators": "ORDER,LIMIT", "complexity": "low", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER", "non_incr_reason": "op:LIMIT,ORDER"}
SELECT * FROM OORDER ORDER BY O_OL_CNT ASC LIMIT 5;
