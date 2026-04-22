-- {"operators": "INNER_JOIN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM,STOCK", "ducklake": true}
SELECT s.S_W_ID, s.S_I_ID, i.I_NAME, (s.S_QUANTITY * i.I_PRICE) AS stock_value, (i.I_PRICE * 0.9) AS discounted_price, (i.I_PRICE + 1.0) AS bump_price FROM dl.STOCK s JOIN dl.ITEM i ON s.S_I_ID = i.I_ID;
