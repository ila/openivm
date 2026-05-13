-- {"operators": "AGGREGATE,ORDER,LIMIT", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ORDER_LINE"}
SELECT OL_W_ID, OL_I_ID, SUM(OL_AMOUNT) AS rev FROM ORDER_LINE GROUP BY OL_W_ID, OL_I_ID ORDER BY rev DESC LIMIT 20;
