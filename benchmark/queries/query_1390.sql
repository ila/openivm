-- {"operators": "ORDER,LIMIT", "complexity": "low", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "STOCK", "non_incr_reason": "op:LIMIT,ORDER"}
SELECT S_W_ID, S_I_ID, S_QUANTITY FROM STOCK ORDER BY S_QUANTITY LIMIT 100;
