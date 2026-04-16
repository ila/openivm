-- {"operators": "INNER_JOIN,AGGREGATE,HAVING,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM,ORDER_LINE"}
SELECT i.I_ID, i.I_NAME, hot.rev FROM ITEM i JOIN (SELECT OL_I_ID, SUM(OL_AMOUNT) AS rev FROM ORDER_LINE GROUP BY OL_I_ID HAVING SUM(OL_AMOUNT) > 50) hot ON i.I_ID = hot.OL_I_ID;
