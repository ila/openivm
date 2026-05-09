-- {"operators": "AGGREGATE,HAVING,UNION", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "DISTRICT,CUSTOMER", "delta": true}
SELECT C_W_ID AS w, COUNT(*) AS n FROM d_CUSTOMER GROUP BY C_W_ID HAVING COUNT(*) > 200 UNION ALL SELECT D_W_ID, COUNT(*) FROM d_DISTRICT GROUP BY D_W_ID HAVING COUNT(*) > 5;
