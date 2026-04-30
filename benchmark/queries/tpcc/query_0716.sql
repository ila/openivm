-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT C_CREDIT, MAX(C_BALANCE) AS max_val FROM CUSTOMER GROUP BY C_CREDIT;
