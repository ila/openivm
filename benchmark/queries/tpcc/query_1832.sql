-- {"operators": "OUTER_JOIN,AGGREGATE,HAVING,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "DISTRICT,OORDER"}
SELECT d.D_W_ID, COALESCE(agg.n, 0) AS n FROM DISTRICT d LEFT JOIN (SELECT O_W_ID, COUNT(*) AS n FROM OORDER GROUP BY O_W_ID HAVING COUNT(*) > 0) agg ON d.D_W_ID = agg.O_W_ID;
