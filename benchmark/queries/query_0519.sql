-- {"operators": "AGGREGATE,ORDER,WINDOW", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "DISTRICT", "openivm_verified": true}
SELECT D_W_ID, D_ID, D_YTD, SUM(D_YTD) OVER (PARTITION BY D_W_ID ORDER BY D_ID ROWS UNBOUNDED PRECEDING) AS cumulative_ytd FROM DISTRICT;
