-- {"operators": "INNER_JOIN", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM,OORDER,ORDER_LINE", "delta": true}
SELECT o.O_ID, ol.OL_I_ID, i.I_NAME FROM d_OORDER o JOIN d_ORDER_LINE ol ON o.O_ID = ol.OL_O_ID JOIN d_ITEM i ON ol.OL_I_ID = i.I_ID;
