-- {"operators": "AGGREGATE,ORDER,WINDOW", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "HISTORY", "openivm_verified": true}
SELECT H_W_ID, H_C_ID, H_AMOUNT, AVG(H_AMOUNT) OVER (PARTITION BY H_W_ID ORDER BY H_DATE ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS rolling_avg FROM HISTORY;
