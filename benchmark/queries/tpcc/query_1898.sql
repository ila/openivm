-- {"operators": "FILTER,CORRELATED_SUBQUERY", "complexity": "medium", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT", "non_incr_reason": "op:CORRELATED_SUBQUERY"}
SELECT w.W_ID FROM WAREHOUSE w WHERE NOT EXISTS (SELECT 1 FROM DISTRICT d WHERE d.D_W_ID = w.W_ID AND d.D_TAX > 0.15);
