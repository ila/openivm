-- {"operators": "OUTER_JOIN,AGGREGATE,HAVING,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "STOCK,ORDER_LINE"}
SELECT s.S_W_ID, COALESCE(agg.n, 0) AS n FROM STOCK s LEFT JOIN (SELECT OL_I_ID, COUNT(*) AS n FROM ORDER_LINE GROUP BY OL_I_ID HAVING COUNT(*) > 0) agg ON s.S_W_ID = agg.OL_I_ID;
