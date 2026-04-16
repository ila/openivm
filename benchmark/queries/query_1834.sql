-- {"operators": "OUTER_JOIN,AGGREGATE,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "CUSTOMER,OORDER"}
SELECT c.C_ID, COALESCE(agg.tot, 0) AS tot FROM CUSTOMER c LEFT JOIN (SELECT O_C_ID, SUM(O_ID) AS tot FROM OORDER GROUP BY O_C_ID) agg ON c.C_ID = agg.O_C_ID;
