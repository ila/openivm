-- {"operators": "PIVOT,AGGREGATE,INNER_JOIN", "complexity": "high", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM,STOCK", "non_incr_reason": "op:PIVOT"}
SELECT * FROM (PIVOT (SELECT s.S_W_ID, i.I_IM_ID, s.S_QUANTITY FROM STOCK s JOIN ITEM i ON s.S_I_ID = i.I_ID) ON S_W_ID IN (1, 2, 3) USING SUM(S_QUANTITY) GROUP BY I_IM_ID) p;
