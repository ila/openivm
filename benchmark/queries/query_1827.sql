-- {"operators": "INNER_JOIN,AGGREGATE,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,OORDER"}
SELECT w.W_ID, agg.avg_val FROM WAREHOUSE w JOIN (SELECT O_W_ID, AVG(O_ID) AS avg_val FROM OORDER GROUP BY O_W_ID) agg ON w.W_ID = agg.O_W_ID;
