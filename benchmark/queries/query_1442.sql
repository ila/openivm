-- {"operators": "AGGREGATE,ORDER,WINDOW", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER", "openivm_verified": true}
SELECT C_W_ID, C_STATE, COUNT(*) AS cnt, NTILE(4) OVER (ORDER BY COUNT(*)) AS quartile FROM CUSTOMER GROUP BY C_W_ID, C_STATE;
