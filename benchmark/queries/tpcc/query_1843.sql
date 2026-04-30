-- {"operators": "OUTER_JOIN,AGGREGATE,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "ITEM,STOCK"}
SELECT i.I_ID, COALESCE(agg.tot, 0) AS tot FROM ITEM i LEFT JOIN (SELECT S_I_ID, SUM(S_W_ID) AS tot FROM STOCK GROUP BY S_I_ID) agg ON i.I_ID = agg.S_I_ID;
