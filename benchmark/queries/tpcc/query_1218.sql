-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": true, "tables": "ORDER_LINE"}
SELECT OL_W_ID, OL_I_ID, SUM(OL_QUANTITY) AS total_qty, CASE WHEN SUM(OL_QUANTITY) > 100 THEN 'popular' ELSE 'regular' END AS label FROM ORDER_LINE GROUP BY OL_W_ID, OL_I_ID;
