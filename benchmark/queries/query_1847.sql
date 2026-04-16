-- {"operators": "OUTER_JOIN,AGGREGATE,HAVING,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "ITEM,ORDER_LINE"}
SELECT i.I_ID, COALESCE(agg.n, 0) AS n FROM ITEM i LEFT JOIN (SELECT OL_I_ID, COUNT(*) AS n FROM ORDER_LINE GROUP BY OL_I_ID HAVING COUNT(*) > 0) agg ON i.I_ID = agg.OL_I_ID;
