-- {"operators": "AGGREGATE,FILTER,SUBQUERY", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT * FROM (SELECT C_W_ID, SUM(C_BALANCE) as total, COUNT(*) as cnt FROM CUSTOMER WHERE C_PAYMENT_CNT > 0 GROUP BY C_W_ID) WHERE total < 0 AND cnt > 1;
