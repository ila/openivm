-- {"operators": "OUTER_JOIN,AGGREGATE,HAVING,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,OORDER"}
SELECT w.W_ID, COALESCE(agg.n, 0) AS n FROM WAREHOUSE w LEFT JOIN (SELECT O_W_ID, COUNT(*) AS n FROM OORDER GROUP BY O_W_ID HAVING COUNT(*) > 0) agg ON w.W_ID = agg.O_W_ID;
