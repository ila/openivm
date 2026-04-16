-- {"operators": "INNER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER,HISTORY"}
SELECT c.C_W_ID, COUNT(*) AS payments, SUM(h.H_AMOUNT) AS total, AVG(h.H_AMOUNT) AS avg_pay, MAX(h.H_AMOUNT) AS biggest FROM CUSTOMER c JOIN HISTORY h ON c.C_ID = h.H_C_ID AND c.C_W_ID = h.H_C_W_ID GROUP BY c.C_W_ID;
