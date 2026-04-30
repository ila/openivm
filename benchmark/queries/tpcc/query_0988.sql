-- {"operators": "FILTER", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "STOCK"}
SELECT S_W_ID, S_QUANTITY FROM STOCK WHERE S_QUANTITY >= 50 AND S_QUANTITY IS NOT NULL;
