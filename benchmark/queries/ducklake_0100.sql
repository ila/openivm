-- {"operators": "INNER_JOIN,DISTINCT", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER,ORDER_LINE", "ducklake": true}
SELECT DISTINCT o.O_W_ID, o.O_D_ID, ol.OL_I_ID FROM OORDER o JOIN ORDER_LINE ol ON o.O_W_ID = ol.OL_W_ID AND o.O_D_ID = ol.OL_D_ID AND o.O_ID = ol.OL_O_ID;
