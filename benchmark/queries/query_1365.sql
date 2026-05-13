-- {"operators": "INNER_JOIN,LATERAL,AGGREGATE,FILTER,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER,HISTORY"}
SELECT c.C_ID, c.C_W_ID, lat.pay_total FROM CUSTOMER c JOIN LATERAL (SELECT SUM(H_AMOUNT) AS pay_total FROM HISTORY WHERE H_C_ID = c.C_ID AND H_C_W_ID = c.C_W_ID) lat ON TRUE;
