-- {"operators": "AGGREGATE,ORDER,LIMIT", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER", "ivm_type": "RECOMPUTE"}
SELECT C_W_ID, C_STATE, COUNT(*) AS n FROM CUSTOMER GROUP BY C_W_ID, C_STATE ORDER BY n DESC LIMIT 25;
