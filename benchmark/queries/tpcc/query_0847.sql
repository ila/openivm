-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER"}
SELECT MIN(O_OL_CNT) AS min_val, MAX(O_OL_CNT) AS max_val FROM OORDER;
