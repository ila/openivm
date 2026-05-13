-- {"operators": "FILTER,ORDER", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER"}
SELECT * FROM OORDER WHERE O_W_ID > 0 ORDER BY O_W_ID;
