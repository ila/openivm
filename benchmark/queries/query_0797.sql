-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ORDER_LINE"}
SELECT OL_O_ID, COUNT(*) AS cnt, SUM(OL_AMOUNT) AS total, AVG(OL_AMOUNT) AS avg_val, MIN(OL_AMOUNT) AS min_val, MAX(OL_AMOUNT) AS max_val FROM ORDER_LINE GROUP BY OL_O_ID;
