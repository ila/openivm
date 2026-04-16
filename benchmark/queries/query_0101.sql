-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "STOCK"}
SELECT S_QUANTITY, COUNT(*) FROM STOCK GROUP BY S_QUANTITY;
