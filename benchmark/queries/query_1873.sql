-- {"operators": "AGGREGATE,ORDER", "complexity": "low", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER", "non_incr_reason": "fn:MODE"}
SELECT C_W_ID, MODE() WITHIN GROUP (ORDER BY C_CREDIT) AS mode_credit FROM CUSTOMER GROUP BY C_W_ID;
