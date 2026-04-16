-- {"operators": "OUTER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM,ORDER_LINE"}
SELECT ol.OL_W_ID, ol.OL_I_ID, COUNT(i.I_ID) AS item_matched, SUM(ol.OL_AMOUNT) AS revenue FROM ITEM i RIGHT JOIN ORDER_LINE ol ON i.I_ID = ol.OL_I_ID GROUP BY ol.OL_W_ID, ol.OL_I_ID;
