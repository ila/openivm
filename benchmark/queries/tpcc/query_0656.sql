-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT C_W_ID, BOOL_AND(C_BALANCE > 0) AS all_positive, BOOL_OR(C_CREDIT = 'BC') AS has_bad_credit FROM CUSTOMER GROUP BY C_W_ID;
