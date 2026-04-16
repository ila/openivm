-- {"operators": "AGGREGATE,DISTINCT", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "HISTORY"}
SELECT H_D_ID, COUNT(DISTINCT H_AMOUNT) AS unique_vals FROM HISTORY GROUP BY H_D_ID;
