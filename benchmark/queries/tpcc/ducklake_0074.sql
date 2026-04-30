-- {"operators": "INNER_JOIN,AGGREGATE,WINDOW", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM,STOCK", "ducklake": true}
SELECT i.I_ID, i.I_NAME, s.S_W_ID, s.S_QUANTITY, AVG(s.S_QUANTITY) OVER (PARTITION BY s.S_W_ID) AS avg_w_qty FROM ITEM i JOIN STOCK s ON i.I_ID = s.S_I_ID;
