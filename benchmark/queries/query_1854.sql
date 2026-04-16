-- {"operators": "INNER_JOIN,AGGREGATE,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER,NEW_ORDER"}
SELECT o.O_ID, agg.avg_val FROM OORDER o JOIN (SELECT NO_O_ID, AVG(NO_O_ID) AS avg_val FROM NEW_ORDER GROUP BY NO_O_ID) agg ON o.O_ID = agg.NO_O_ID;
