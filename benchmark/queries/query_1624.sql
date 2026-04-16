-- {"operators": "INNER_JOIN,AGGREGATE,FILTER,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,CUSTOMER"}
SELECT w.W_ID, w.W_NAME, sub.avg_bal FROM WAREHOUSE w JOIN (SELECT C_W_ID, AVG(C_BALANCE) AS avg_bal FROM CUSTOMER WHERE C_CREDIT = 'GC' GROUP BY C_W_ID) sub ON w.W_ID = sub.C_W_ID;
