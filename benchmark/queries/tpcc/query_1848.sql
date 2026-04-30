-- {"operators": "INNER_JOIN,AGGREGATE,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM,ORDER_LINE"}
SELECT i.I_ID, agg.avg_val FROM ITEM i JOIN (SELECT OL_I_ID, AVG(OL_W_ID) AS avg_val FROM ORDER_LINE GROUP BY OL_I_ID) agg ON i.I_ID = agg.OL_I_ID;
