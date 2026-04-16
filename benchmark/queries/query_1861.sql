-- {"operators": "AGGREGATE,FILTER", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "OORDER"}
SELECT O_W_ID, MIN(O_ENTRY_D) AS earliest, MAX(O_ENTRY_D) AS latest, DATE_DIFF('day', MIN(O_ENTRY_D), MAX(O_ENTRY_D)) AS span_days FROM OORDER WHERE O_ENTRY_D IS NOT NULL GROUP BY O_W_ID;
