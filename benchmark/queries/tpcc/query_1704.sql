-- {"operators": "INNER_JOIN,AGGREGATE,DISTINCT", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER,ORDER_LINE"}
SELECT o.O_ID, MIN(ol.OL_NUMBER) AS mn, MAX(ol.OL_NUMBER) AS mx, COUNT(DISTINCT ol.OL_NUMBER) AS uniq FROM OORDER o JOIN ORDER_LINE ol ON o.O_ID = ol.OL_O_ID AND o.O_W_ID = ol.OL_W_ID GROUP BY o.O_ID;
