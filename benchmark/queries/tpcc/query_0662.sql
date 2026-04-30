-- {"operators": "AGGREGATE,ORDER,DISTINCT", "complexity": "medium", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER", "non_incr_reason": "fn:STRING_AGG"}
SELECT C_W_ID, STRING_AGG(DISTINCT C_CREDIT, ',' ORDER BY C_CREDIT) AS credit_types, COUNT(*) AS cnt FROM CUSTOMER GROUP BY C_W_ID;
