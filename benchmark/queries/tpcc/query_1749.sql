-- {"operators": "OUTER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "OORDER,NEW_ORDER"}
SELECT o.O_ID, COUNT(*) AS n, SUM(COALESCE(1, 0)) AS tot, AVG(1) AS avg_val FROM OORDER o RIGHT JOIN NEW_ORDER no ON o.O_ID = no.NO_O_ID AND o.O_W_ID = no.NO_W_ID GROUP BY o.O_ID;
