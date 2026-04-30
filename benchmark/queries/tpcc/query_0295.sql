-- {"operators": "OUTER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,STOCK"}
SELECT COUNT(*) FROM WAREHOUSE LEFT JOIN STOCK ON WAREHOUSE.W_ID = STOCK.S_W_ID;
