-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "DISTRICT"}
SELECT MIN(D_YTD) AS min_val, MAX(D_YTD) AS max_val FROM DISTRICT;
