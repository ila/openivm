-- {"operators": "FILTER", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "DISTRICT"}
SELECT D_W_ID, D_YTD FROM DISTRICT WHERE D_YTD >= 0 AND D_YTD IS NOT NULL;
