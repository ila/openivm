-- {"operators": "INNER_JOIN,AGGREGATE,HAVING", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "DISTRICT,CUSTOMER"}
SELECT d.D_W_ID, COUNT(*) AS n FROM DISTRICT d JOIN CUSTOMER c ON d.D_W_ID = c.C_W_ID AND d.D_ID = c.C_D_ID GROUP BY d.D_W_ID HAVING COUNT(*) > 0;
