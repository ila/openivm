-- {"operators": "RIGHT_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "ITEM,STOCK", "openivm_verified": true}
SELECT i.I_ID, COUNT(s.S_W_ID) AS stock_rows, SUM(COALESCE(s.S_QUANTITY, 0)) AS stock_qty FROM ITEM i RIGHT JOIN STOCK s ON i.I_ID = s.S_I_ID GROUP BY i.I_ID;
