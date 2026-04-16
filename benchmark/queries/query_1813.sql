-- {"operators": "INNER_JOIN,ORDER,WINDOW", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "STOCK,ORDER_LINE", "openivm_verified": true}
SELECT s.S_W_ID, ol.OL_O_ID, ROW_NUMBER() OVER (PARTITION BY s.S_W_ID ORDER BY ol.OL_O_ID DESC) AS rn FROM STOCK s JOIN ORDER_LINE ol ON s.S_I_ID = ol.OL_I_ID AND s.S_W_ID = ol.OL_SUPPLY_W_ID;
