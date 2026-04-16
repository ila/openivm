-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER"}
SELECT O_W_ID, COUNT(*) AS cnt, SUM(O_OL_CNT) AS total, AVG(O_OL_CNT) AS avg_val, MIN(O_OL_CNT) AS min_val, MAX(O_OL_CNT) AS max_val FROM OORDER GROUP BY O_W_ID;
