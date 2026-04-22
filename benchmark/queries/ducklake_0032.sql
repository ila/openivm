-- {"operators": "INNER_JOIN,AGGREGATE,FILTER,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT", "ducklake": true}
SELECT w.W_ID, w.W_NAME, t.dcount FROM WAREHOUSE w JOIN (SELECT D_W_ID, COUNT(*) AS dcount FROM DISTRICT GROUP BY D_W_ID) t ON w.W_ID = t.D_W_ID WHERE t.dcount > 5;
