-- {"operators": "OUTER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT"}
SELECT w.W_ID, w.W_NAME, COUNT(d.D_ID) AS districts, SUM(d.D_YTD) AS total_ytd, AVG(d.D_TAX) AS avg_tax FROM WAREHOUSE w LEFT JOIN DISTRICT d ON w.W_ID = d.D_W_ID GROUP BY w.W_ID, w.W_NAME;
