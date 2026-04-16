-- {"operators": "OUTER_JOIN,AGGREGATE,LIMIT", "complexity": "medium", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT", "non_incr_reason": "op:LIMIT"}
SELECT WAREHOUSE.W_ID, COUNT(*) FROM WAREHOUSE LEFT JOIN DISTRICT ON WAREHOUSE.W_ID = DISTRICT.D_W_ID GROUP BY WAREHOUSE.W_ID LIMIT 10;
