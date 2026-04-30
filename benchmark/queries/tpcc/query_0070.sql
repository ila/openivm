-- {"operators": "AGGREGATE,FILTER", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT SUM(C_BALANCE) FROM CUSTOMER WHERE C_BALANCE < 0 AND C_PAYMENT_CNT > 0;
