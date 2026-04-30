-- {"operators": "INNER_JOIN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM,OORDER,ORDER_LINE", "ducklake": true}
SELECT o.O_W_ID AS w, o.O_ID AS oid, ol.OL_NUMBER AS line_num, ol.OL_I_ID AS item, ol.OL_AMOUNT AS amt FROM OORDER o JOIN ORDER_LINE ol ON o.O_W_ID = ol.OL_W_ID AND o.O_D_ID = ol.OL_D_ID AND o.O_ID = ol.OL_O_ID;
