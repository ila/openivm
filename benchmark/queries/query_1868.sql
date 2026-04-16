-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": false, "has_nulls": false, "has_cast": true, "has_case": false, "tables": "CUSTOMER", "non_incr_reason": "fn:COVAR_POP"}
SELECT C_W_ID, COVAR_POP(C_BALANCE, CAST(C_YTD_PAYMENT AS DOUBLE)) AS cov, COVAR_SAMP(C_BALANCE, CAST(C_YTD_PAYMENT AS DOUBLE)) AS cov_samp FROM CUSTOMER GROUP BY C_W_ID;
