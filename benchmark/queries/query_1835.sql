-- {"operators": "OUTER_JOIN,AGGREGATE,HAVING,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "CUSTOMER,OORDER"}
SELECT c.C_ID, COALESCE(agg.n, 0) AS n FROM CUSTOMER c LEFT JOIN (SELECT O_C_ID, COUNT(*) AS n FROM OORDER GROUP BY O_C_ID HAVING COUNT(*) > 0) agg ON c.C_ID = agg.O_C_ID;
