-- {"operators": "AGGREGATE,ORDER,LIMIT", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT C_W_ID, C_STATE, AVG(C_BALANCE) AS avg_b FROM CUSTOMER GROUP BY C_W_ID, C_STATE ORDER BY avg_b DESC LIMIT 15;
