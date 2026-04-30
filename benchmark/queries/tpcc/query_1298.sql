-- {"operators": "AGGREGATE,WINDOW", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "NEW_ORDER", "openivm_verified": true}
SELECT NO_W_ID, NO_O_ID, SUM(NO_O_ID) OVER (PARTITION BY NO_W_ID) AS part_sum FROM NEW_ORDER;
