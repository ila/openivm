-- {"operators": "INNER_JOIN", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "DISTRICT,OORDER,NEW_ORDER"}
SELECT o.O_ID, o.O_W_ID, d.D_NAME, no.NO_O_ID FROM OORDER o JOIN NEW_ORDER no ON o.O_ID = no.NO_O_ID AND o.O_W_ID = no.NO_W_ID JOIN DISTRICT d ON o.O_W_ID = d.D_W_ID AND o.O_D_ID = d.D_ID;
