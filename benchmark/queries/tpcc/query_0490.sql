-- {"operators": "ORDER,LIMIT", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "STOCK", "ivm_type": "RECOMPUTE"}
SELECT * FROM STOCK ORDER BY S_QUANTITY ASC LIMIT 5;
