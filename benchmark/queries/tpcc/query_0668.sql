-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER"}
SELECT O_W_ID, O_ID, O_ENTRY_D, O_ENTRY_D + INTERVAL '30 days' AS due_date FROM OORDER;
