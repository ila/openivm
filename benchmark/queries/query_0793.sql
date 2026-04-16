-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ORDER_LINE", "openivm_verified": true}
SELECT OL_O_ID, VARIANCE(OL_AMOUNT) AS var_val FROM ORDER_LINE GROUP BY OL_O_ID;
