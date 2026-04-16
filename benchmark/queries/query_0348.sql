-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": true, "tables": "STOCK"}
SELECT S_I_ID, CASE WHEN S_QUANTITY = 0 THEN 'out_of_stock' WHEN S_QUANTITY < 100 THEN 'low_stock' WHEN S_QUANTITY < 500 THEN 'medium_stock' ELSE 'high_stock' END as stock_level FROM STOCK;
