-- {"operators": "OUTER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT", "ducklake": true}
SELECT w.W_ID, w.W_NAME, COUNT(d.D_ID) AS dcount FROM dl.WAREHOUSE w LEFT JOIN dl.DISTRICT d ON w.W_ID = d.D_W_ID GROUP BY w.W_ID, w.W_NAME;
