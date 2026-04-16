-- {"operators": "OUTER_JOIN,AGGREGATE,TABLE_FUNCTION", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "STOCK"}
SELECT r.n AS benchmark, COUNT(s.S_W_ID) AS low_stock_items FROM range(5, 55, 5) r(n) LEFT JOIN STOCK s ON s.S_QUANTITY < r.n GROUP BY r.n;
