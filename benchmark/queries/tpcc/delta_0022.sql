-- {"operators": "INNER_JOIN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM,STOCK", "delta": true}
SELECT s.S_W_ID AS w, s.S_I_ID AS i, i.I_NAME AS item, i.I_PRICE AS price, s.S_QUANTITY AS stock_qty FROM d_STOCK s JOIN d_ITEM i ON s.S_I_ID = i.I_ID;
