-- {"operators": "OUTER_JOIN,AGGREGATE,HAVING,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,CUSTOMER"}
SELECT w.W_ID, COALESCE(agg.n, 0) AS n FROM WAREHOUSE w LEFT JOIN (SELECT C_W_ID, COUNT(*) AS n FROM CUSTOMER GROUP BY C_W_ID HAVING COUNT(*) > 0) agg ON w.W_ID = agg.C_W_ID;
