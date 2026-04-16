-- {"operators": "INNER_JOIN", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT,CUSTOMER,OORDER,ORDER_LINE"}
SELECT * FROM WAREHOUSE w JOIN DISTRICT d ON w.W_ID = d.D_W_ID JOIN CUSTOMER c ON d.D_W_ID = c.C_W_ID JOIN OORDER o ON c.C_ID = o.O_C_ID JOIN ORDER_LINE ol ON o.O_ID = ol.OL_O_ID;
