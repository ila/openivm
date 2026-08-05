-- {"operators": "AGGREGATE,CASE", "complexity": "medium", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": true, "tables": "ORDER_LINE", "ducklake": true}
SELECT OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER,
       SUM(CASE WHEN OL_DELIVERY_D IS NULL THEN OL_AMOUNT ELSE NULL END) AS pending_amount
FROM dl.ORDER_LINE
GROUP BY OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER;
