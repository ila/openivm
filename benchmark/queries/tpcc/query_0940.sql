-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM"}
SELECT I_IM_ID, COUNT(*) AS cnt, SUM(I_PRICE) AS total, AVG(I_PRICE) AS avg_val, MIN(I_PRICE) AS min_val, MAX(I_PRICE) AS max_val FROM ITEM GROUP BY I_IM_ID;
