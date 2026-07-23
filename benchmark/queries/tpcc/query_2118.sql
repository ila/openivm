-- {"operators": "SAMPLE,LEFT_JOIN,AGGREGATE", "complexity": "high", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM,STOCK", "non_incr_reason": "op:SAMPLE"}
SELECT i.I_IM_ID, COUNT(s.S_I_ID) AS sampled_stock_rows FROM ITEM i LEFT JOIN (SELECT * FROM STOCK USING SAMPLE reservoir(30 ROWS) REPEATABLE (17)) s ON i.I_ID = s.S_I_ID GROUP BY i.I_IM_ID;
