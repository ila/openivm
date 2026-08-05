-- {"operators": "LEFT_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT", "openivm_verified": true}
SELECT w.W_ID, COUNT(d.D_ID) AS district_count, COALESCE(SUM(d.D_YTD), 0) AS district_ytd FROM WAREHOUSE w LEFT JOIN DISTRICT d ON w.W_ID = d.D_W_ID GROUP BY w.W_ID;
