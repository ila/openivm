-- {"operators": "AGGREGATE,WINDOW", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "HISTORY", "openivm_verified": true}
SELECT H_W_ID, H_AMOUNT, AVG(H_AMOUNT) OVER (PARTITION BY H_W_ID) AS part_avg FROM HISTORY;
