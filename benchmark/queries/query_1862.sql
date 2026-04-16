-- {"operators": "AGGREGATE,FILTER", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "HISTORY"}
SELECT H_W_ID, EXTRACT(ISODOW FROM H_DATE) AS iso_dow, COUNT(*) AS n FROM HISTORY WHERE H_DATE IS NOT NULL GROUP BY H_W_ID, EXTRACT(ISODOW FROM H_DATE);
