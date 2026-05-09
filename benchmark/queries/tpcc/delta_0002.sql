-- {"operators": "INNER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT", "delta": true}
SELECT d.D_W_ID, SUM(d.D_YTD) AS total_ytd FROM d_WAREHOUSE w JOIN d_DISTRICT d ON w.W_ID = d.D_W_ID GROUP BY d.D_W_ID;
