-- {"operators": "INNER_JOIN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,OORDER"}
SELECT * FROM WAREHOUSE INNER JOIN OORDER ON WAREHOUSE.W_ID = OORDER.O_W_ID;
