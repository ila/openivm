-- {"operators": "INNER_JOIN,FILTER", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM,ORDER_LINE", "openivm_verified": true}
SELECT i.I_ID, i.I_PRICE, ol.OL_AMOUNT, ol.OL_QUANTITY FROM ITEM i JOIN ORDER_LINE ol ON i.I_ID = ol.OL_I_ID WHERE ol.OL_AMOUNT >= i.I_PRICE;
