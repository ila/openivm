-- {"operators": "OUTER_JOIN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER,NEW_ORDER"}
SELECT no.NO_W_ID, no.NO_D_ID, no.NO_O_ID, o.O_ENTRY_D FROM NEW_ORDER no RIGHT JOIN OORDER o ON no.NO_O_ID = o.O_ID AND no.NO_W_ID = o.O_W_ID;
