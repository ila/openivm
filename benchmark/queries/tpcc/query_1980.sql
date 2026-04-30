-- {"operators": "AGGREGATE,HAVING,DISTINCT", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT c.C_W_ID, COUNT(DISTINCT c.C_STATE) AS states FROM CUSTOMER c GROUP BY c.C_W_ID HAVING COUNT(DISTINCT c.C_STATE) > 5;
