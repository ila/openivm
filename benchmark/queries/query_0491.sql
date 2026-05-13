-- {"operators": "ORDER,LIMIT", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER"}
SELECT * FROM OORDER ORDER BY O_OL_CNT ASC LIMIT 5;
