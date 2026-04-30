-- {"operators": "ORDER,WINDOW", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "HISTORY", "openivm_verified": true}
SELECT H_W_ID, H_AMOUNT, RANK() OVER (PARTITION BY H_W_ID ORDER BY H_AMOUNT) AS rnk FROM HISTORY;
