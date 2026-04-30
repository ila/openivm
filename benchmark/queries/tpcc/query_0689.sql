-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT C_D_ID, MIN(C_BALANCE) AS min_val FROM CUSTOMER GROUP BY C_D_ID;
