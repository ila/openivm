-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER"}
SELECT O_C_ID, SUM(O_OL_CNT) AS total FROM OORDER GROUP BY O_C_ID;
