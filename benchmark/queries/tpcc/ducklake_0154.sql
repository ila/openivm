-- {"operators": "OUTER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT", "ducklake": true}
SELECT w.W_ID, w.W_NAME, COALESCE(SUM(d.D_YTD), 0) AS sum_ytd, COALESCE(COUNT(d.D_ID), 0) AS dcount FROM dl.WAREHOUSE w LEFT JOIN dl.DISTRICT d ON w.W_ID = d.D_W_ID GROUP BY w.W_ID, w.W_NAME;
