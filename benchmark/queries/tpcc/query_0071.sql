-- {"operators": "AGGREGATE,FILTER", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "STOCK"}
SELECT AVG(S_QUANTITY) FROM STOCK WHERE S_QUANTITY > 50 AND S_QUANTITY < 500;
