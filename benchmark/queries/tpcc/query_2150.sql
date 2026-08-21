-- {"operators": "ANTI_JOIN,NOT_IN", "complexity": "medium", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "ORDER_LINE,OORDER", "openivm_verified": true}
SELECT OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER FROM ORDER_LINE WHERE OL_NUMBER NOT IN (SELECT O_CARRIER_ID FROM OORDER);
