-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "HISTORY"}
SELECT H_D_ID, SUM(H_AMOUNT) AS total FROM HISTORY GROUP BY H_D_ID;
