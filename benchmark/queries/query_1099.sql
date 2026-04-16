-- {"operators": "INNER_JOIN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM,STOCK"}
SELECT i.I_ID, i.I_NAME, i.I_PRICE, s.S_W_ID, s.S_QUANTITY FROM ITEM i JOIN STOCK s ON i.I_ID = s.S_I_ID;
