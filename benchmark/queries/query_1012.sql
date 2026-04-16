-- {"operators": "FILTER", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "WAREHOUSE"}
SELECT W_STATE, W_YTD FROM WAREHOUSE WHERE W_YTD >= 0 AND W_YTD IS NOT NULL;
