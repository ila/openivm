-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "STOCK"}
SELECT MIN(S_QUANTITY) AS min_val, MAX(S_QUANTITY) AS max_val FROM STOCK;
