-- {"operators": "AGGREGATE,FILTER", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "DISTRICT"}
SELECT D_W_ID, COUNT(*) AS cnt, SUM(D_YTD) AS total FROM DISTRICT WHERE D_YTD > 0 GROUP BY D_W_ID;
