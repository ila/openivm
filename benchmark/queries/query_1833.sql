-- {"operators": "INNER_JOIN,AGGREGATE,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "DISTRICT,OORDER"}
SELECT d.D_W_ID, agg.avg_val FROM DISTRICT d JOIN (SELECT O_W_ID, AVG(O_ID) AS avg_val FROM OORDER GROUP BY O_W_ID) agg ON d.D_W_ID = agg.O_W_ID;
