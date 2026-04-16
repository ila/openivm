-- {"operators": "INNER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,OORDER"}
SELECT WAREHOUSE.W_ID, COUNT(*) FROM WAREHOUSE JOIN OORDER ON WAREHOUSE.W_ID = OORDER.O_W_ID GROUP BY WAREHOUSE.W_ID;
