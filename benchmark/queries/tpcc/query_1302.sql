-- {"operators": "ORDER,WINDOW", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "NEW_ORDER", "openivm_verified": true}
SELECT NO_W_ID, NO_O_ID, LEAD(NO_O_ID, 1) OVER (PARTITION BY NO_W_ID ORDER BY NO_O_ID) AS nxt FROM NEW_ORDER;
