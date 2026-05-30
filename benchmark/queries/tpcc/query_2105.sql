-- {"operators": "PIVOT,AGGREGATE,FILTER", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ORDER_LINE"}
SELECT * FROM (PIVOT (SELECT OL_W_ID, OL_I_ID, OL_AMOUNT FROM ORDER_LINE WHERE OL_AMOUNT > 0) ON OL_W_ID IN (1, 2, 3) USING SUM(OL_AMOUNT) GROUP BY OL_I_ID) p;
