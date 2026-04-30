-- {"operators": "FILTER", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": true, "has_case": false, "tables": "OORDER"}
SELECT O_W_ID, O_ID, CAST(O_ENTRY_D AS DATE) AS entry_date, CAST(O_ENTRY_D AS TIMESTAMP) AS entry_ts FROM OORDER WHERE O_ENTRY_D IS NOT NULL;
