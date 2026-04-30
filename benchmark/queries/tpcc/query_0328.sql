-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT C_BALANCE / NULLIF(C_PAYMENT_CNT, 0) as avg_payment_amt FROM CUSTOMER;
