-- {"operators": "FILTER,SEMI_JOIN,ANY", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": true, "has_case": false, "tables": "STOCK,ORDER_LINE"}
SELECT s.S_W_ID, s.S_I_ID FROM STOCK s WHERE s.S_QUANTITY > ANY (SELECT CAST(ol.OL_QUANTITY AS INT) FROM ORDER_LINE ol WHERE ol.OL_I_ID = s.S_I_ID);
