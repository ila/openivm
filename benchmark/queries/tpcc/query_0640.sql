-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": true, "has_case": true, "tables": "DISTRICT"}
SELECT D_W_ID, D_ID, D_NAME, D_YTD, CAST(D_YTD AS BIGINT) AS ytd_int, D_TAX, CASE WHEN D_NEXT_O_ID > 1000 THEN 'busy' ELSE 'quiet' END AS activity FROM DISTRICT;
