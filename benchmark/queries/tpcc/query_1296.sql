-- {"operators": "ORDER,WINDOW", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "NEW_ORDER", "openivm_verified": true}
SELECT NO_W_ID, NO_O_ID, ROW_NUMBER() OVER (PARTITION BY NO_W_ID ORDER BY NO_O_ID DESC) AS rn FROM NEW_ORDER;
