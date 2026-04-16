-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "STOCK"}
SELECT S_W_ID, MAX(S_QUANTITY) AS max_val FROM STOCK GROUP BY S_W_ID;
