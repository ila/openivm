-- {"operators": "ANTI_JOIN,NOT_IN", "complexity": "high", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "ORDER_LINE,OORDER", "openivm_verified": true}
SELECT ol.OL_W_ID, ol.OL_D_ID, ol.OL_O_ID, ol.OL_NUMBER FROM ORDER_LINE ol WHERE ol.OL_NUMBER NOT IN (SELECT o.O_CARRIER_ID FROM OORDER o WHERE o.O_W_ID = ol.OL_W_ID AND o.O_D_ID = ol.OL_D_ID);
