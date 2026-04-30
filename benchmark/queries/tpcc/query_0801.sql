-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ORDER_LINE"}
SELECT COUNT(*) AS cnt, SUM(OL_AMOUNT) AS total, AVG(OL_AMOUNT) AS avg_val FROM ORDER_LINE;
