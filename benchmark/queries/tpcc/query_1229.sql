-- {"operators": "FILTER", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "OORDER"}
SELECT O_W_ID, O_ID, O_ENTRY_D + INTERVAL '7 days' AS week_later FROM OORDER WHERE O_ENTRY_D IS NOT NULL;
