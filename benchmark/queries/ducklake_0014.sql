-- {"operators": "INNER_JOIN,FILTER", "complexity": "medium", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "OORDER,ORDER_LINE", "ducklake": true}
SELECT ol.OL_W_ID, ol.OL_D_ID, ol.OL_O_ID, ol.OL_NUMBER, ol.OL_AMOUNT FROM OORDER o JOIN ORDER_LINE ol ON o.O_W_ID = ol.OL_W_ID AND o.O_D_ID = ol.OL_D_ID AND o.O_ID = ol.OL_O_ID WHERE o.O_CARRIER_ID IS NULL;
