-- {"operators": "OUTER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT", "delta": true}
SELECT COUNT(*) FROM d_WAREHOUSE LEFT JOIN d_DISTRICT ON WAREHOUSE.W_ID = DISTRICT.D_W_ID;
