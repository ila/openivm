-- {"operators": "ORDER,LIMIT", "complexity": "low", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE", "non_incr_reason": "op:LIMIT,ORDER"}
SELECT W_ID, W_NAME, W_YTD FROM WAREHOUSE ORDER BY W_YTD DESC LIMIT 5;
