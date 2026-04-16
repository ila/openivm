-- {"operators": "INNER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM,ORDER_LINE"}
SELECT i.I_IM_ID, COUNT(*) AS sales, SUM(ol.OL_AMOUNT) AS revenue, MIN(ol.OL_AMOUNT) AS min_sale, MAX(ol.OL_AMOUNT) AS max_sale FROM ITEM i JOIN ORDER_LINE ol ON i.I_ID = ol.OL_I_ID GROUP BY i.I_IM_ID;
