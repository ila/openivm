-- {"operators": "OUTER_JOIN,AGGREGATE,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "OORDER,ORDER_LINE"}
SELECT o.O_ID, COALESCE(agg.tot, 0) AS tot FROM OORDER o LEFT JOIN (SELECT OL_O_ID, SUM(OL_NUMBER) AS tot FROM ORDER_LINE GROUP BY OL_O_ID) agg ON o.O_ID = agg.OL_O_ID;
