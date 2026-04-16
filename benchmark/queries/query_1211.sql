-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": true, "tables": "STOCK"}
SELECT S_W_ID, SUM(CASE WHEN S_QUANTITY < 20 THEN 1 ELSE 0 END) AS low_stock, SUM(CASE WHEN S_QUANTITY >= 20 THEN 1 ELSE 0 END) AS ok_stock FROM STOCK GROUP BY S_W_ID;
