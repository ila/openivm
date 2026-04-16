-- {"operators": "OUTER_JOIN,AGGREGATE,HAVING,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "OORDER,ORDER_LINE"}
SELECT o.O_ID, COALESCE(agg.n, 0) AS n FROM OORDER o LEFT JOIN (SELECT OL_O_ID, COUNT(*) AS n FROM ORDER_LINE GROUP BY OL_O_ID HAVING COUNT(*) > 0) agg ON o.O_ID = agg.OL_O_ID;
