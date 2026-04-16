-- {"operators": "OUTER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT tier, COUNT(*) AS cust FROM (VALUES ('vip', 5000), ('gold', 1000), ('std', 0)) AS t(tier, threshold) LEFT JOIN CUSTOMER c ON c.C_BALANCE >= t.threshold GROUP BY tier;
