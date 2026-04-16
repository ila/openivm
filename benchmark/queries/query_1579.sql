-- {"operators": "AGGREGATE,FILTER", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "OORDER"}
SELECT O_W_ID, DAYOFWEEK(O_ENTRY_D) AS dow, COUNT(*) AS orders FROM OORDER WHERE O_ENTRY_D IS NOT NULL GROUP BY O_W_ID, DAYOFWEEK(O_ENTRY_D);
