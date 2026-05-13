-- {"operators": "INNER_JOIN,LATERAL,AGGREGATE,FILTER,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "CUSTOMER,HISTORY"}
SELECT c.C_W_ID, c.C_ID, top_pay.amt FROM CUSTOMER c JOIN LATERAL (SELECT MAX(H_AMOUNT) AS amt FROM HISTORY WHERE H_C_ID = c.C_ID AND H_C_W_ID = c.C_W_ID) top_pay ON TRUE WHERE top_pay.amt IS NOT NULL;
