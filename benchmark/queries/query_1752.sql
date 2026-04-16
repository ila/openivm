-- {"operators": "FULL_OUTER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "OORDER,NEW_ORDER"}
SELECT COALESCE(o.O_ID, no.NO_O_ID) AS gk, COUNT(*) AS n, SUM(COALESCE(1, 0)) AS tot FROM OORDER o FULL OUTER JOIN NEW_ORDER no ON o.O_ID = no.NO_O_ID AND o.O_W_ID = no.NO_W_ID GROUP BY COALESCE(o.O_ID, no.NO_O_ID);
