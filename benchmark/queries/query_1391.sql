-- {"operators": "ORDER,LIMIT", "complexity": "low", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER", "non_incr_reason": "op:LIMIT,ORDER"}
SELECT O_W_ID, O_ID, O_ENTRY_D FROM OORDER ORDER BY O_ENTRY_D DESC LIMIT 50;
