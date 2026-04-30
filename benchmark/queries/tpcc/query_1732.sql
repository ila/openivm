-- {"operators": "FULL_OUTER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "ITEM,ORDER_LINE"}
SELECT COALESCE(i.I_ID, ol.OL_W_ID) AS gk, COUNT(*) AS n, SUM(COALESCE(ol.OL_W_ID, 0)) AS tot FROM ITEM i FULL OUTER JOIN ORDER_LINE ol ON i.I_ID = ol.OL_I_ID GROUP BY COALESCE(i.I_ID, ol.OL_W_ID);
