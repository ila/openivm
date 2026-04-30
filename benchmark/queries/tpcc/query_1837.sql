-- {"operators": "OUTER_JOIN,AGGREGATE,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "CUSTOMER,HISTORY"}
SELECT c.C_ID, COALESCE(agg.tot, 0) AS tot FROM CUSTOMER c LEFT JOIN (SELECT H_C_ID, SUM(H_AMOUNT) AS tot FROM HISTORY GROUP BY H_C_ID) agg ON c.C_ID = agg.H_C_ID;
