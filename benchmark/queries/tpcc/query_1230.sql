-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT C_W_ID, ABS(SUM(C_BALANCE)) AS abs_total, ROUND(AVG(C_BALANCE), 2) AS rnd_avg, FLOOR(MIN(C_BALANCE)) AS flr_min, CEIL(MAX(C_BALANCE)) AS cel_max FROM CUSTOMER GROUP BY C_W_ID;
