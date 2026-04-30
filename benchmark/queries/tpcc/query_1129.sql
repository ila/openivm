-- {"operators": "OUTER_JOIN,AGGREGATE,HAVING", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER,NEW_ORDER"}
SELECT o.O_ID, COUNT(no.NO_O_ID) AS cnt FROM OORDER o LEFT JOIN NEW_ORDER no ON o.O_ID = no.NO_O_ID AND o.O_W_ID = no.NO_W_ID GROUP BY o.O_ID HAVING COUNT(no.NO_O_ID) > 0;
