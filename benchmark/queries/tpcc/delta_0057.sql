-- {"operators": "OUTER_JOIN,AGGREGATE,HAVING", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT", "delta": true}
SELECT w.W_ID, w.W_STATE, COUNT(d.D_ID) AS dcnt FROM d_WAREHOUSE w LEFT JOIN d_DISTRICT d ON w.W_ID = d.D_W_ID AND d.D_YTD > 0 GROUP BY w.W_ID, w.W_STATE HAVING COUNT(d.D_ID) > 0;
