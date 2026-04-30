-- {"operators": "INNER_JOIN,AGGREGATE,HAVING", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER,OORDER"}
SELECT c.C_W_ID, c.C_D_ID, COUNT(o.O_ID) AS orders FROM CUSTOMER c JOIN OORDER o ON c.C_ID = o.O_C_ID AND c.C_W_ID = o.O_W_ID GROUP BY c.C_W_ID, c.C_D_ID HAVING COUNT(o.O_ID) > 1;
