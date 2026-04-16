-- {"operators": "INNER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "ITEM,ORDER_LINE"}
SELECT i.I_ID, COUNT(*) AS n, SUM(COALESCE(ol.OL_W_ID, 0)) AS tot, AVG(ol.OL_W_ID) AS avg_val FROM ITEM i JOIN ORDER_LINE ol ON i.I_ID = ol.OL_I_ID GROUP BY i.I_ID;
