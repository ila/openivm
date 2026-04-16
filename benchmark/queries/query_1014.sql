-- {"operators": "FILTER", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE"}
SELECT W_STATE, W_YTD FROM WAREHOUSE WHERE W_YTD BETWEEN 0 AND 100000;
