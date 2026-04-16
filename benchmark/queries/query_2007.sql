-- {"operators": "AGGREGATE,ORDER,LIMIT", "complexity": "medium", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER", "non_incr_reason": "op:LIMIT,ORDER"}
SELECT C_W_ID, C_STATE, COUNT(*) AS n FROM CUSTOMER GROUP BY C_W_ID, C_STATE ORDER BY n DESC LIMIT 25;
