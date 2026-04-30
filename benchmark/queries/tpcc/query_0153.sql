-- {"operators": "FILTER,ORDER", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE"}
SELECT * FROM WAREHOUSE WHERE W_ID > 0 ORDER BY W_ID;
