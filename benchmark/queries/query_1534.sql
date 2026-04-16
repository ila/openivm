-- {"operators": "OUTER_JOIN,AGGREGATE,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "ITEM,ORDER_LINE"}
SELECT i.I_ID, i.I_NAME, i.I_PRICE, COALESCE(ir.rev, 0) AS revenue FROM ITEM i LEFT JOIN (SELECT OL_I_ID, SUM(OL_AMOUNT) AS rev FROM ORDER_LINE GROUP BY OL_I_ID) ir ON i.I_ID = ir.OL_I_ID;
