-- {"operators": "INNER_JOIN,AGGREGATE", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM,STOCK,ORDER_LINE"}
SELECT i.I_ID, s.S_W_ID, COUNT(*) AS cnt FROM ITEM i JOIN STOCK s ON i.I_ID = s.S_I_ID JOIN ORDER_LINE ol ON ol.OL_I_ID = i.I_ID AND ol.OL_SUPPLY_W_ID = s.S_W_ID GROUP BY i.I_ID, s.S_W_ID;
