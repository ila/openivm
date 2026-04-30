-- {"operators": "FILTER", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": true, "has_case": false, "tables": "HISTORY"}
SELECT H_W_ID, H_C_ID, H_AMOUNT, H_DATE, CAST(H_DATE AS DATE) AS payment_date, ROUND(H_AMOUNT, 0) AS rounded_amt FROM HISTORY WHERE H_AMOUNT IS NOT NULL;
