-- {"operators": "INNER_JOIN,AGGREGATE,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER,OORDER"}
SELECT c.C_ID, agg.avg_val FROM CUSTOMER c JOIN (SELECT O_C_ID, AVG(O_ID) AS avg_val FROM OORDER GROUP BY O_C_ID) agg ON c.C_ID = agg.O_C_ID;
