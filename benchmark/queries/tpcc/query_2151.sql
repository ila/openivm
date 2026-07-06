-- {"operators": "SEMI_JOIN,IN", "complexity": "medium", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "OORDER,ORDER_LINE", "openivm_verified": true}
SELECT O_W_ID, O_D_ID, O_ID, O_CARRIER_ID FROM OORDER WHERE O_CARRIER_ID IN (SELECT OL_NUMBER FROM ORDER_LINE WHERE OL_AMOUNT > 0);
