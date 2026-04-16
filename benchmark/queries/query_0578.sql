-- {"operators": "INNER_JOIN,AGGREGATE,TABLE_FUNCTION", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "STOCK"}
SELECT r.n AS target_qty, COUNT(*) AS items_below FROM range(10, 110, 10) r(n) JOIN STOCK s ON s.S_QUANTITY < r.n GROUP BY r.n;
