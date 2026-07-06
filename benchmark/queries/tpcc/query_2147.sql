-- {"operators": "FULL_OUTER_JOIN", "complexity": "high", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "ITEM,STOCK", "openivm_verified": true}
SELECT COALESCE(i.I_ID, s.S_I_ID) AS item_id, i.I_NAME, s.S_W_ID, s.S_QUANTITY FROM ITEM i FULL OUTER JOIN STOCK s ON i.I_ID = s.S_I_ID;
