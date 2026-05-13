-- {"operators": "INNER_JOIN,LATERAL,AGGREGATE,FILTER,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM,STOCK"}
SELECT i.I_ID, i.I_NAME, lat.stock_total FROM ITEM i JOIN LATERAL (SELECT SUM(S_QUANTITY) AS stock_total FROM STOCK WHERE S_I_ID = i.I_ID) lat ON TRUE WHERE lat.stock_total > 0;
