-- {"operators": "CROSS_JOIN,FILTER,UNION,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM"}
SELECT I_ID, I_NAME FROM ITEM CROSS JOIN (SELECT 'A' AS grade UNION ALL SELECT 'B' UNION ALL SELECT 'C') g WHERE I_PRICE > 50 AND g.grade = 'A';
