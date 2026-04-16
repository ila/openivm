-- {"operators": "OUTER_JOIN,AGGREGATE,FILTER,CTE,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM,STOCK"}
WITH s_low AS (SELECT S_W_ID, S_I_ID FROM STOCK WHERE S_QUANTITY < 20) SELECT i.I_ID, i.I_NAME, COUNT(sl.S_I_ID) AS low_warehouses FROM ITEM i LEFT JOIN s_low sl ON i.I_ID = sl.S_I_ID GROUP BY i.I_ID, i.I_NAME;
