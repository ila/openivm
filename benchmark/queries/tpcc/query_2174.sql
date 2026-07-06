-- {"operators": "AGGREGATE,BOOL", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER", "openivm_verified": true}
SELECT C_W_ID, BOOL_OR(C_BALANCE > 0) AS has_positive_balance, BOOL_AND(C_PAYMENT_CNT >= 0) AS all_nonnegative_payments FROM CUSTOMER GROUP BY C_W_ID;
