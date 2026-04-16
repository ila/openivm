-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER", "non_incr_reason": "fn:QUANTILE"}
SELECT C_W_ID, APPROX_QUANTILE(C_BALANCE, 0.5) AS median_approx, APPROX_QUANTILE(C_BALANCE, 0.9) AS p90 FROM CUSTOMER GROUP BY C_W_ID;
