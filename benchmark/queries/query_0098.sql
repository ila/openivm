-- {"operators": "AGGREGATE,DISTINCT", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT COUNT(*), COUNT(DISTINCT C_BALANCE), SUM(C_BALANCE), AVG(C_BALANCE), MIN(C_BALANCE), MAX(C_BALANCE) FROM CUSTOMER;
