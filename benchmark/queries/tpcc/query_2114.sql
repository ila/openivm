-- {"operators": "SAMPLE", "complexity": "medium", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "STOCK", "non_incr_reason": "op:SAMPLE"}
SELECT S_W_ID, S_I_ID, S_QUANTITY FROM STOCK USING SAMPLE reservoir(10 ROWS) REPEATABLE (42);
