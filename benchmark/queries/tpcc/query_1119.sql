-- {"operators": "INNER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "STOCK,ORDER_LINE"}
SELECT s.S_W_ID, COUNT(*) AS cnt FROM STOCK s JOIN ORDER_LINE ol ON s.S_I_ID = ol.OL_I_ID AND s.S_W_ID = ol.OL_SUPPLY_W_ID GROUP BY s.S_W_ID;
