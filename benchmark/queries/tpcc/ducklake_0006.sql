-- {"operators": "INNER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER,NEW_ORDER", "ducklake": true}
SELECT o.O_W_ID, o.O_D_ID, COUNT(*) AS new_orders FROM OORDER o JOIN NEW_ORDER no ON o.O_W_ID = no.NO_W_ID AND o.O_D_ID = no.NO_D_ID AND o.O_ID = no.NO_O_ID GROUP BY o.O_W_ID, o.O_D_ID;
