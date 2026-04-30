-- {"operators": "OUTER_JOIN,AGGREGATE,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,CUSTOMER"}
SELECT w.W_ID, COALESCE(agg.tot, 0) AS tot FROM WAREHOUSE w LEFT JOIN (SELECT C_W_ID, SUM(C_ID) AS tot FROM CUSTOMER GROUP BY C_W_ID) agg ON w.W_ID = agg.C_W_ID;
