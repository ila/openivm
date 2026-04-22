-- {"operators": "ORDER,WINDOW", "complexity": "medium", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER", "non_incr_reason": "op:ORDER", "ducklake": true}
SELECT O_W_ID, O_ID, O_ENTRY_D, LAG(O_ENTRY_D) OVER (PARTITION BY O_W_ID ORDER BY O_ID) AS prev_entry, LEAD(O_ENTRY_D) OVER (PARTITION BY O_W_ID ORDER BY O_ID) AS next_entry FROM dl.OORDER;
