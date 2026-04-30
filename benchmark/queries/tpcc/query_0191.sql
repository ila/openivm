-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "DISTRICT"}
SELECT D_YTD, COUNT(*) FROM DISTRICT GROUP BY D_YTD;
