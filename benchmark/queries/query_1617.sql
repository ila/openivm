-- {"operators": "AGGREGATE,FILTER,UNION", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "STOCK"}
SELECT 'in_stock' AS status, S_W_ID, COUNT(*) AS cnt FROM STOCK WHERE S_QUANTITY > 0 GROUP BY S_W_ID UNION ALL SELECT 'out_of_stock', S_W_ID, COUNT(*) FROM STOCK WHERE S_QUANTITY = 0 GROUP BY S_W_ID;
