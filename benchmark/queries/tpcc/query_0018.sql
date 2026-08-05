-- {"operators": "INNER_JOIN,FILTER", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER,OORDER,ORDER_LINE"}
SELECT *
FROM CUSTOMER c
JOIN OORDER o ON c.C_W_ID = o.O_W_ID AND c.C_D_ID = o.O_D_ID AND c.C_ID = o.O_C_ID
JOIN ORDER_LINE ol ON o.O_W_ID = ol.OL_W_ID AND o.O_D_ID = ol.OL_D_ID AND o.O_ID = ol.OL_O_ID
WHERE c.C_W_ID = 1 AND c.C_D_ID <= 2 AND c.C_ID <= 20;
