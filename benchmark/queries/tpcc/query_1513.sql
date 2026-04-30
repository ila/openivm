-- {"operators": "INNER_JOIN,AGGREGATE,HAVING", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM,ORDER_LINE"}
SELECT i.I_ID, i.I_NAME, SUM(ol.OL_QUANTITY) AS qty FROM ITEM i JOIN ORDER_LINE ol ON i.I_ID = ol.OL_I_ID GROUP BY i.I_ID, i.I_NAME HAVING SUM(ol.OL_QUANTITY) > 5 AND COUNT(*) > 2;
