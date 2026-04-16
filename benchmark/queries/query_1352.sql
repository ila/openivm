-- {"operators": "FULL_OUTER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "ITEM,STOCK"}
SELECT COALESCE(i.I_ID, s.S_I_ID) AS item_id, i.I_NAME, SUM(s.S_QUANTITY) AS total_stock FROM ITEM i FULL OUTER JOIN STOCK s ON i.I_ID = s.S_I_ID GROUP BY COALESCE(i.I_ID, s.S_I_ID), i.I_NAME;
