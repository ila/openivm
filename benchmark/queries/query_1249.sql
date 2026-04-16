-- {"operators": "AGGREGATE,WINDOW", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "STOCK", "openivm_verified": true}
SELECT S_W_ID, S_QUANTITY, SUM(S_QUANTITY) OVER (PARTITION BY S_W_ID) AS part_sum FROM STOCK;
