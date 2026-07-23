-- {"operators": "SAMPLE,PIVOT,AGGREGATE", "complexity": "high", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "STOCK", "non_incr_reason": "op:SAMPLE"}
SELECT * FROM (PIVOT (SELECT * FROM STOCK USING SAMPLE reservoir(20 ROWS) REPEATABLE (29)) ON S_W_ID IN (1, 2, 3) USING SUM(S_QUANTITY) GROUP BY S_I_ID) p;
