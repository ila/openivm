-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER", "openivm_verified": true}
SELECT C_D_ID, STDDEV(C_BALANCE) AS std_val FROM CUSTOMER GROUP BY C_D_ID;
