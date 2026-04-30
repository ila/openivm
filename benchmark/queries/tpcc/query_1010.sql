-- {"operators": "FILTER", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "DISTRICT"}
SELECT D_W_ID, D_YTD FROM DISTRICT WHERE D_YTD BETWEEN 0 AND 100000;
