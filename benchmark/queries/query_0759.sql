-- {"operators": "AGGREGATE,FILTER", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ORDER_LINE"}
SELECT OL_W_ID, COUNT(*) AS cnt, SUM(OL_AMOUNT) AS total FROM ORDER_LINE WHERE OL_AMOUNT > 10 GROUP BY OL_W_ID;
