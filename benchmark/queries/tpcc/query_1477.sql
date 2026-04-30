-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ORDER_LINE", "openivm_verified": true}
SELECT OL_W_ID, OL_I_ID, SUM(OL_AMOUNT) AS rev FROM ORDER_LINE GROUP BY GROUPING SETS ((OL_W_ID), (OL_I_ID), (OL_W_ID, OL_I_ID));
