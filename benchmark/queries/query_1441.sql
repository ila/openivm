-- {"operators": "AGGREGATE,ORDER,WINDOW", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "HISTORY", "openivm_verified": true}
SELECT H_W_ID, H_D_ID, SUM(H_AMOUNT) AS tot, LAG(SUM(H_AMOUNT)) OVER (PARTITION BY H_W_ID ORDER BY H_D_ID) AS prev FROM HISTORY GROUP BY H_W_ID, H_D_ID;
