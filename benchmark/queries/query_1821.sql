-- {"operators": "INNER_JOIN,AGGREGATE,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT"}
SELECT w.W_ID, agg.avg_val FROM WAREHOUSE w JOIN (SELECT D_W_ID, AVG(D_ID) AS avg_val FROM DISTRICT GROUP BY D_W_ID) agg ON w.W_ID = agg.D_W_ID;
