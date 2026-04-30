-- {"operators": "OUTER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT t.n, t.label, COUNT(c.C_ID) AS cust FROM (VALUES (1, 'small'), (2, 'medium'), (3, 'large')) AS t(n, label) LEFT JOIN CUSTOMER c ON c.C_W_ID = t.n GROUP BY t.n, t.label;
