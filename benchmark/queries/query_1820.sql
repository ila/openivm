-- {"operators": "OUTER_JOIN,AGGREGATE,HAVING,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT"}
SELECT w.W_ID, COALESCE(agg.n, 0) AS n FROM WAREHOUSE w LEFT JOIN (SELECT D_W_ID, COUNT(*) AS n FROM DISTRICT GROUP BY D_W_ID HAVING COUNT(*) > 0) agg ON w.W_ID = agg.D_W_ID;
