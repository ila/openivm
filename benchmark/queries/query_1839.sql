-- {"operators": "INNER_JOIN,AGGREGATE,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER,HISTORY"}
SELECT c.C_ID, agg.avg_val FROM CUSTOMER c JOIN (SELECT H_C_ID, AVG(H_AMOUNT) AS avg_val FROM HISTORY GROUP BY H_C_ID) agg ON c.C_ID = agg.H_C_ID;
