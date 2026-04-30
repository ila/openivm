-- {"operators": "FILTER", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": true, "has_case": false, "tables": "OORDER"}
SELECT O_W_ID, O_ID, O_ENTRY_D, CAST(O_ENTRY_D AS DATE) AS entry_date FROM OORDER WHERE O_ENTRY_D IS NOT NULL;
