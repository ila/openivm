-- {"operators": "PIVOT,AGGREGATE,LEFT_JOIN", "complexity": "high", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "ITEM,STOCK"}
SELECT * FROM (PIVOT (SELECT i.I_IM_ID, s.S_W_ID, COALESCE(s.S_QUANTITY, 0) AS qty FROM ITEM i LEFT JOIN STOCK s ON i.I_ID = s.S_I_ID) ON S_W_ID IN (1, 2, 3) USING SUM(qty) GROUP BY I_IM_ID) p;
