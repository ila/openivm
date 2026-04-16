-- {"operators": "AGGREGATE,UNION", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ORDER_LINE,HISTORY"}
SELECT OL_W_ID AS w_id, SUM(OL_AMOUNT) AS total FROM ORDER_LINE GROUP BY OL_W_ID UNION ALL SELECT H_W_ID, SUM(H_AMOUNT) FROM HISTORY GROUP BY H_W_ID;
