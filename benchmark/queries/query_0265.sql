-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER"}
SELECT O_OL_CNT, O_OL_CNT, COUNT(*) FROM OORDER GROUP BY O_OL_CNT, O_OL_CNT;
