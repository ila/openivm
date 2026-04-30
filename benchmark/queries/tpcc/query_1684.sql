-- {"operators": "INNER_JOIN,AGGREGATE,DISTINCT", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER,OORDER"}
SELECT c.C_ID, MIN(o.O_OL_CNT) AS mn, MAX(o.O_OL_CNT) AS mx, COUNT(DISTINCT o.O_OL_CNT) AS uniq FROM CUSTOMER c JOIN OORDER o ON c.C_ID = o.O_C_ID AND c.C_W_ID = o.O_W_ID GROUP BY c.C_ID;
