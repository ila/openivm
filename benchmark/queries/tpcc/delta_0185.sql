-- {"operators": "INNER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "DISTRICT,CUSTOMER", "delta": true}
SELECT c.C_W_ID, c.C_CREDIT, COUNT(*) AS n, SUM(c.C_BALANCE) AS total FROM d_CUSTOMER c JOIN d_DISTRICT d ON c.C_W_ID = d.D_W_ID AND c.C_D_ID = d.D_ID GROUP BY c.C_W_ID, c.C_CREDIT;
