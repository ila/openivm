-- {"operators": "OUTER_JOIN,AGGREGATE,DISTINCT", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER,OORDER"}
SELECT c.C_W_ID, COUNT(DISTINCT o.O_ID) FROM CUSTOMER c LEFT JOIN OORDER o ON c.C_ID = o.O_C_ID GROUP BY c.C_W_ID;
