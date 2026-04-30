-- {"operators": "AGGREGATE,WINDOW,SUBQUERY", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ORDER_LINE", "openivm_verified": true}
SELECT w_id, total, total / SUM(total) OVER () AS pct_of_all FROM (SELECT OL_W_ID AS w_id, SUM(OL_AMOUNT) AS total FROM ORDER_LINE GROUP BY OL_W_ID) sub;
