-- {"operators": "WINDOW", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ORDER_LINE"}
SELECT OL_W_ID, OL_D_ID, OL_O_ID, OL_AMOUNT, SUM(OL_AMOUNT) OVER (PARTITION BY OL_W_ID, OL_D_ID ORDER BY OL_O_ID) AS running_revenue FROM ORDER_LINE;
