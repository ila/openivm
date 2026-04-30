-- {"operators": "AGGREGATE,WINDOW", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ORDER_LINE"}
SELECT OL_W_ID, OL_D_ID, OL_O_ID, SUM(OL_AMOUNT) AS order_total, COUNT(*) AS line_count, RANK() OVER (PARTITION BY OL_W_ID, OL_D_ID ORDER BY SUM(OL_AMOUNT) DESC) AS revenue_rank FROM ORDER_LINE GROUP BY OL_W_ID, OL_D_ID, OL_O_ID;
