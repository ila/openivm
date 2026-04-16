-- {"operators": "AGGREGATE,HAVING", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT C_CREDIT, COUNT(*) AS cnt, AVG(C_BALANCE) AS avg_val FROM CUSTOMER GROUP BY C_CREDIT HAVING COUNT(*) > 0 AND AVG(C_BALANCE) > 0;
