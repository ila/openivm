-- {"operators": "OUTER_JOIN,AGGREGATE,TABLE_FUNCTION", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT s.i AS scale, COUNT(c.C_ID) AS customers FROM generate_series(1, 5) s(i) LEFT JOIN CUSTOMER c ON c.C_W_ID <= s.i GROUP BY s.i;
