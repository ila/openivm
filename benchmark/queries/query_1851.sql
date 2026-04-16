-- {"operators": "INNER_JOIN,AGGREGATE,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "STOCK,ORDER_LINE"}
SELECT s.S_W_ID, agg.avg_val FROM STOCK s JOIN (SELECT OL_I_ID, AVG(OL_O_ID) AS avg_val FROM ORDER_LINE GROUP BY OL_I_ID) agg ON s.S_W_ID = agg.OL_I_ID;
