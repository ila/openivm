-- {"operators": "FULL_OUTER_JOIN", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "ITEM,STOCK"}
SELECT COALESCE(i.I_ID, s.S_I_ID) AS item_id, i.I_NAME, i.I_PRICE, COALESCE(s.S_QUANTITY, 0) AS qty FROM ITEM i FULL OUTER JOIN STOCK s ON i.I_ID = s.S_I_ID;
