-- {"operators": "AGGREGATE,FILTER", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT COUNT(*) FROM CUSTOMER WHERE C_W_ID = 1 AND C_BALANCE < -1000 AND C_PAYMENT_CNT > 1;
