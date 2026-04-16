-- {"operators": "AGGREGATE,WINDOW", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE", "openivm_verified": true}
SELECT W_ID, W_NAME, W_YTD, W_TAX, ABS(W_YTD - AVG(W_YTD) OVER ()) AS deviation FROM WAREHOUSE;
