-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE"}
SELECT MIN(W_YTD) AS min_val, MAX(W_YTD) AS max_val FROM WAREHOUSE;
