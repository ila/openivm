-- {"operators": "OUTER_JOIN,AGGREGATE,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "OORDER,NEW_ORDER"}
SELECT o.O_ID, COALESCE(agg.tot, 0) AS tot FROM OORDER o LEFT JOIN (SELECT NO_O_ID, SUM(NO_O_ID) AS tot FROM NEW_ORDER GROUP BY NO_O_ID) agg ON o.O_ID = agg.NO_O_ID;
