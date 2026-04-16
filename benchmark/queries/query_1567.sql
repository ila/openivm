-- {"operators": "AGGREGATE,ORDER", "complexity": "low", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER", "non_incr_reason": "fn:STRING_AGG"}
SELECT C_W_ID, STRING_AGG(C_LAST, ',' ORDER BY C_LAST) AS all_names FROM CUSTOMER GROUP BY C_W_ID;
