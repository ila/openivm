-- {"operators": "OUTER_JOIN,AGGREGATE,HAVING,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "ITEM,STOCK"}
SELECT i.I_ID, COALESCE(agg.n, 0) AS n FROM ITEM i LEFT JOIN (SELECT S_I_ID, COUNT(*) AS n FROM STOCK GROUP BY S_I_ID HAVING COUNT(*) > 0) agg ON i.I_ID = agg.S_I_ID;
