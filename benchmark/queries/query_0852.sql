-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "DISTRICT"}
SELECT D_W_ID, SUM(D_YTD) AS total FROM DISTRICT GROUP BY D_W_ID;
