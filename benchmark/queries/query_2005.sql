-- {"operators": "ORDER,LIMIT", "complexity": "low", "is_incremental": false, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "DISTRICT", "non_incr_reason": "op:LIMIT,ORDER"}
SELECT D_NAME, D_YTD FROM DISTRICT ORDER BY D_YTD DESC NULLS LAST LIMIT 30;
