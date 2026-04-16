-- {"operators": "FILTER", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "HISTORY"}
SELECT H_W_ID, H_C_ID, H_DATE, EXTRACT(YEAR FROM H_DATE) AS yr, EXTRACT(MONTH FROM H_DATE) AS mo FROM HISTORY WHERE H_DATE IS NOT NULL;
