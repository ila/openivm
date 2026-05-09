-- {"operators": "INNER_JOIN", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER,OORDER,ORDER_LINE", "delta": true}
SELECT c.C_ID, o.O_ID, ol.OL_AMOUNT FROM d_CUSTOMER c JOIN d_OORDER o ON c.C_ID = o.O_C_ID JOIN d_ORDER_LINE ol ON o.O_ID = ol.OL_O_ID;
