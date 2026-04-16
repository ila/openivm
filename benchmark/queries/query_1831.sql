-- {"operators": "OUTER_JOIN,AGGREGATE,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "DISTRICT,OORDER"}
SELECT d.D_W_ID, COALESCE(agg.tot, 0) AS tot FROM DISTRICT d LEFT JOIN (SELECT O_W_ID, SUM(O_ID) AS tot FROM OORDER GROUP BY O_W_ID) agg ON d.D_W_ID = agg.O_W_ID;
