-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT C_STATE, SUM(C_BALANCE) AS total FROM CUSTOMER GROUP BY C_STATE;
