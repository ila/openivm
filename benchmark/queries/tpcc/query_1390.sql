-- {"operators": "ORDER,LIMIT", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "STOCK", "ivm_type": "RECOMPUTE"}
SELECT S_W_ID, S_I_ID, S_QUANTITY FROM STOCK ORDER BY S_QUANTITY LIMIT 100;
