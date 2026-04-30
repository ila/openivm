-- {"operators": "OUTER_JOIN,AGGREGATE,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "ITEM,ORDER_LINE"}
SELECT i.I_ID, COALESCE(agg.tot, 0) AS tot FROM ITEM i LEFT JOIN (SELECT OL_I_ID, SUM(OL_W_ID) AS tot FROM ORDER_LINE GROUP BY OL_I_ID) agg ON i.I_ID = agg.OL_I_ID;
