-- {"operators": "OUTER_JOIN,AGGREGATE,TABLE_FUNCTION", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER"}
SELECT g.m AS month_num, COUNT(o.O_ID) AS orders FROM generate_series(1, 12) g(m) LEFT JOIN OORDER o ON EXTRACT(MONTH FROM o.O_ENTRY_D) = g.m GROUP BY g.m;
