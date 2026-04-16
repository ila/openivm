-- {"operators": "INNER_JOIN,AGGREGATE,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER,ORDER_LINE"}
SELECT o.O_W_ID, o.O_ID, ot.total FROM OORDER o JOIN (SELECT OL_W_ID, OL_O_ID, SUM(OL_AMOUNT) AS total FROM ORDER_LINE GROUP BY OL_W_ID, OL_O_ID) ot ON o.O_W_ID = ot.OL_W_ID AND o.O_ID = ot.OL_O_ID;
