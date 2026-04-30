-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT COUNT(*) AS cnt, SUM(C_BALANCE) AS total, AVG(C_BALANCE) AS avg_val FROM CUSTOMER;
