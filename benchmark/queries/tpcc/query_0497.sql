-- {"operators": "UNION", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT"}
SELECT W_ID FROM WAREHOUSE UNION ALL SELECT D_W_ID FROM DISTRICT;
