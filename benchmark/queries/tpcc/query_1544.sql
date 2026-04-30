-- {"operators": "INNER_JOIN,AGGREGATE,HAVING", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER,HISTORY"}
SELECT c.C_STATE, COUNT(*) AS payments, SUM(h.H_AMOUNT) AS total FROM CUSTOMER c JOIN HISTORY h ON c.C_ID = h.H_C_ID AND c.C_W_ID = h.H_C_W_ID GROUP BY c.C_STATE HAVING SUM(h.H_AMOUNT) > 100 OR COUNT(*) > 3;
