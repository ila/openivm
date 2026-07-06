-- {"operators": "INNER_JOIN,FILTER", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "STOCK,ORDER_LINE", "openivm_verified": true}
SELECT s.S_W_ID, s.S_I_ID, ol.OL_O_ID, ol.OL_AMOUNT FROM STOCK s JOIN ORDER_LINE ol ON s.S_W_ID = ol.OL_SUPPLY_W_ID AND s.S_I_ID = ol.OL_I_ID WHERE s.S_QUANTITY >= ol.OL_QUANTITY;
