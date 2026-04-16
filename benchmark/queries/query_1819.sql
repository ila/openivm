-- {"operators": "OUTER_JOIN,AGGREGATE,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT"}
SELECT w.W_ID, COALESCE(agg.tot, 0) AS tot FROM WAREHOUSE w LEFT JOIN (SELECT D_W_ID, SUM(D_ID) AS tot FROM DISTRICT GROUP BY D_W_ID) agg ON w.W_ID = agg.D_W_ID;
