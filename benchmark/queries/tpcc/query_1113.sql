-- {"operators": "OUTER_JOIN,AGGREGATE,HAVING", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM,ORDER_LINE"}
SELECT i.I_ID, COUNT(ol.OL_W_ID) AS cnt FROM ITEM i LEFT JOIN ORDER_LINE ol ON i.I_ID = ol.OL_I_ID GROUP BY i.I_ID HAVING COUNT(ol.OL_W_ID) > 0;
