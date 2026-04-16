-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT C_W_ID, COUNT(*) AS cnt, SUM(C_BALANCE) AS total, AVG(C_BALANCE) AS avg_val, MIN(C_BALANCE) AS min_val, MAX(C_BALANCE) AS max_val FROM CUSTOMER GROUP BY C_W_ID;
