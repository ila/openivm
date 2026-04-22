-- {"operators": "INNER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "DISTRICT,CUSTOMER", "ducklake": true}
SELECT c.C_W_ID, c.C_CREDIT, COUNT(*) AS n, SUM(c.C_BALANCE) AS total FROM dl.CUSTOMER c JOIN dl.DISTRICT d ON c.C_W_ID = d.D_W_ID AND c.C_D_ID = d.D_ID GROUP BY c.C_W_ID, c.C_CREDIT;
