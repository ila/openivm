-- {"operators": "AGGREGATE,ORDER", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ORDER_LINE", "openivm_verified": true}
SELECT OL_O_ID, OL_W_ID, LIST(OL_I_ID ORDER BY OL_NUMBER) AS item_sequence, SUM(OL_AMOUNT) AS tot FROM ORDER_LINE GROUP BY OL_O_ID, OL_W_ID;
