-- {"operators": "AGGREGATE,FILTER", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": true, "has_case": false, "tables": "OORDER"}
SELECT O_W_ID, COUNT(*) FROM OORDER WHERE O_ENTRY_D < CAST('2025-01-01' AS TIMESTAMP) GROUP BY O_W_ID;
