-- {"operators": "OUTER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "ITEM,ORDER_LINE"}
SELECT i.I_ID, i.I_NAME, COUNT(ol.OL_NUMBER) AS times_ordered, COALESCE(SUM(ol.OL_AMOUNT), 0) AS revenue FROM ITEM i LEFT JOIN ORDER_LINE ol ON i.I_ID = ol.OL_I_ID GROUP BY i.I_ID, i.I_NAME;
