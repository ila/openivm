-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER"}
SELECT COUNT(*) AS cnt, SUM(O_OL_CNT) AS total, AVG(O_OL_CNT) AS avg_val FROM OORDER;
