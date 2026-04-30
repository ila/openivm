-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ORDER_LINE", "openivm_verified": true}
SELECT OL_D_ID, VAR_POP(OL_AMOUNT) AS var_pop FROM ORDER_LINE GROUP BY OL_D_ID;
