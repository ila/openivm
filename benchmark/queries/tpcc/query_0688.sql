-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT C_D_ID, AVG(C_BALANCE) AS avg_val FROM CUSTOMER GROUP BY C_D_ID;
