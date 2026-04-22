-- {"operators": "INNER_JOIN,FILTER", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER,NEW_ORDER", "ducklake": true}
SELECT o.O_W_ID, o.O_D_ID, o.O_ID, o.O_C_ID FROM dl.OORDER o JOIN dl.NEW_ORDER no ON o.O_W_ID = no.NO_W_ID AND o.O_D_ID = no.NO_D_ID AND o.O_ID = no.NO_O_ID WHERE o.O_OL_CNT > 3;
