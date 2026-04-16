-- {"operators": "FILTER", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "NEW_ORDER"}
SELECT NO_W_ID, NO_O_ID FROM NEW_ORDER WHERE NO_O_ID >= 0 AND NO_O_ID IS NOT NULL;
