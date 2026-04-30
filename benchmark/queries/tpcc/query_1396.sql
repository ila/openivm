-- {"operators": "ORDER,LIMIT", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE", "ivm_type": "RECOMPUTE"}
SELECT W_ID, W_NAME, W_YTD FROM WAREHOUSE ORDER BY W_YTD DESC LIMIT 5;
