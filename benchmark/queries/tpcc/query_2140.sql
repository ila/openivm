-- {"operators": "AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER", "openivm_verified": true}
SELECT C_W_ID, C_CREDIT, AVG(C_BALANCE::DOUBLE) AS avg_balance, STDDEV(C_BALANCE::DOUBLE) AS std_balance FROM CUSTOMER GROUP BY C_W_ID, C_CREDIT;
