-- {"operators": "OUTER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,OORDER"}
SELECT COUNT(*) FROM WAREHOUSE LEFT JOIN OORDER ON WAREHOUSE.W_ID = OORDER.O_W_ID;
