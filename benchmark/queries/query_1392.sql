-- {"operators": "AGGREGATE,ORDER,LIMIT", "complexity": "medium", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER", "non_incr_reason": "op:LIMIT,ORDER"}
SELECT C_W_ID, COUNT(*) AS cnt FROM CUSTOMER GROUP BY C_W_ID ORDER BY cnt DESC LIMIT 10;
