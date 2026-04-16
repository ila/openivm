-- {"operators": "INNER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,STOCK"}
SELECT WAREHOUSE.W_ID, COUNT(*) FROM WAREHOUSE INNER JOIN STOCK ON WAREHOUSE.W_ID = STOCK.S_W_ID GROUP BY WAREHOUSE.W_ID;
