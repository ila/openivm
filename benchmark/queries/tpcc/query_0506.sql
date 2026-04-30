-- {"operators": "ORDER,WINDOW", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE", "openivm_verified": true}
SELECT W_ID, W_YTD, LAG(W_YTD, 1) OVER (ORDER BY W_ID) AS prev_ytd, LEAD(W_YTD, 1) OVER (ORDER BY W_ID) AS next_ytd FROM WAREHOUSE;
