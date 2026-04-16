-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "STOCK"}
SELECT S_W_ID, COUNT(*) AS cnt, SUM(S_QUANTITY) AS total, AVG(S_QUANTITY) AS avg_val, MIN(S_QUANTITY) AS min_val, MAX(S_QUANTITY) AS max_val FROM STOCK GROUP BY S_W_ID;
