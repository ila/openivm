-- {"operators": "INNER_JOIN,AGGREGATE,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER,ORDER_LINE"}
SELECT o.O_ID, agg.avg_val FROM OORDER o JOIN (SELECT OL_O_ID, AVG(OL_NUMBER) AS avg_val FROM ORDER_LINE GROUP BY OL_O_ID) agg ON o.O_ID = agg.OL_O_ID;
