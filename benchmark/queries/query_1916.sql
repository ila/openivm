-- {"operators": "CROSS_JOIN,LATERAL,AGGREGATE,FILTER,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER,ORDER_LINE"}
SELECT o.O_W_ID, o.O_ID, lines.cnt, lines.total FROM OORDER o CROSS JOIN LATERAL (SELECT COUNT(*) AS cnt, SUM(OL_AMOUNT) AS total FROM ORDER_LINE WHERE OL_O_ID = o.O_ID AND OL_W_ID = o.O_W_ID) lines;
