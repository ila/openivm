-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER"}
SELECT O_ID, O_W_ID, O_ENTRY_D, O_ENTRY_D + INTERVAL '1 day' AS plus_day, O_ENTRY_D - INTERVAL '1 hour' AS minus_hour FROM OORDER;
