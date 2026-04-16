-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": false, "has_nulls": false, "has_cast": true, "has_case": false, "tables": "CUSTOMER", "non_incr_reason": "fn:CORR"}
SELECT C_W_ID, CORR(C_BALANCE, CAST(C_YTD_PAYMENT AS DOUBLE)) AS corr_bal_pay FROM CUSTOMER GROUP BY C_W_ID;
