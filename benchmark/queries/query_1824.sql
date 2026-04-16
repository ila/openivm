-- {"operators": "INNER_JOIN,AGGREGATE,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,CUSTOMER"}
SELECT w.W_ID, agg.avg_val FROM WAREHOUSE w JOIN (SELECT C_W_ID, AVG(C_ID) AS avg_val FROM CUSTOMER GROUP BY C_W_ID) agg ON w.W_ID = agg.C_W_ID;
