-- {"operators": "AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "STOCK", "openivm_verified": true}
SELECT S_W_ID, MIN(S_QUANTITY) AS min_qty, MAX(S_QUANTITY) AS max_qty, COUNT(*) AS item_count FROM STOCK GROUP BY S_W_ID;
