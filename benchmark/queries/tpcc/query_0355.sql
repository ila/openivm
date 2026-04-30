-- {"operators": "AGGREGATE,FILTER,CTE,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
WITH step1 AS (SELECT C_W_ID, SUM(C_BALANCE) as total FROM CUSTOMER GROUP BY C_W_ID), step2 AS (SELECT * FROM step1 WHERE total < 0), step3 AS (SELECT COUNT(*) as neg_warehouses FROM step2) SELECT * FROM step3;
