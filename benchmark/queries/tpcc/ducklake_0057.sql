-- {"operators": "OUTER_JOIN,AGGREGATE,HAVING", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT", "ducklake": true}
SELECT w.W_ID, w.W_STATE, COUNT(d.D_ID) AS dcnt FROM WAREHOUSE w LEFT JOIN DISTRICT d ON w.W_ID = d.D_W_ID AND d.D_YTD > 0 GROUP BY w.W_ID, w.W_STATE HAVING COUNT(d.D_ID) > 0;
