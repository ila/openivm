-- {"operators": "INNER_JOIN", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM,OORDER,ORDER_LINE"}
SELECT * FROM OORDER o JOIN ORDER_LINE ol ON o.O_ID = ol.OL_O_ID JOIN ITEM i ON ol.OL_I_ID = i.I_ID;
