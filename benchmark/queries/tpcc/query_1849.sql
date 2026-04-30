-- {"operators": "OUTER_JOIN,AGGREGATE,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "STOCK,ORDER_LINE"}
SELECT s.S_W_ID, COALESCE(agg.tot, 0) AS tot FROM STOCK s LEFT JOIN (SELECT OL_I_ID, SUM(OL_O_ID) AS tot FROM ORDER_LINE GROUP BY OL_I_ID) agg ON s.S_W_ID = agg.OL_I_ID;
