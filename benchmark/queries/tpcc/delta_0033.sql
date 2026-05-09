-- {"operators": "INNER_JOIN,AGGREGATE,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM,ORDER_LINE", "delta": true}
SELECT i.I_ID, i.I_NAME, s.line_count FROM d_ITEM i JOIN (SELECT OL_I_ID, COUNT(*) AS line_count FROM d_ORDER_LINE GROUP BY OL_I_ID) s ON i.I_ID = s.OL_I_ID;
