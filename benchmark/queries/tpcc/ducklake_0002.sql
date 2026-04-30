-- {"operators": "INNER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT", "ducklake": true}
SELECT d.D_W_ID, SUM(d.D_YTD) AS total_ytd FROM WAREHOUSE w JOIN DISTRICT d ON w.W_ID = d.D_W_ID GROUP BY d.D_W_ID;
