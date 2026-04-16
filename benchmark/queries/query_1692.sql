-- {"operators": "FULL_OUTER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "CUSTOMER,OORDER"}
SELECT COALESCE(c.C_ID, o.O_ID) AS gk, COUNT(*) AS n, SUM(COALESCE(o.O_OL_CNT, 0)) AS tot FROM CUSTOMER c FULL OUTER JOIN OORDER o ON c.C_ID = o.O_C_ID AND c.C_W_ID = o.O_W_ID GROUP BY COALESCE(c.C_ID, o.O_ID);
