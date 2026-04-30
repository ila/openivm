-- {"operators": "OUTER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT"}
SELECT COUNT(*) FROM WAREHOUSE LEFT JOIN DISTRICT ON WAREHOUSE.W_ID = DISTRICT.D_W_ID;
