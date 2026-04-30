-- {"operators": "OUTER_JOIN,AGGREGATE,TABLE_FUNCTION", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "STOCK"}
SELECT r.n, COUNT(s.S_W_ID) AS warehouses_with_stock FROM range(1, 101) r(n) LEFT JOIN STOCK s ON s.S_I_ID = r.n GROUP BY r.n;
