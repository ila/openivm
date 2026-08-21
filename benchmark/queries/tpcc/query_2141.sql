-- {"operators": "AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": true, "tables": "CUSTOMER", "openivm_verified": true}
SELECT C_W_ID, SUM(CASE WHEN C_BALANCE < 0 THEN 1 ELSE 0 END) AS negative_customers, SUM(CASE WHEN C_BALANCE >= 0 THEN C_BALANCE ELSE 0 END) AS nonnegative_balance FROM CUSTOMER GROUP BY C_W_ID;
