-- {"operators": "INNER_JOIN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,STOCK"}
SELECT * FROM WAREHOUSE INNER JOIN STOCK ON WAREHOUSE.W_ID = STOCK.S_W_ID;
