-- {"operators": "ORDER,WINDOW", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "DISTRICT", "openivm_verified": true}
SELECT D_W_ID, D_YTD, LEAD(D_YTD, 1) OVER (PARTITION BY D_W_ID ORDER BY D_YTD) AS nxt FROM DISTRICT;
