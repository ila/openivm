-- {"operators": "ORDER,WINDOW", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE", "openivm_verified": true}
SELECT W_STATE, W_YTD, LAG(W_YTD, 1) OVER (PARTITION BY W_STATE ORDER BY W_YTD) AS prev FROM WAREHOUSE;
