-- {"operators": "PIVOT,AGGREGATE", "complexity": "high", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "STOCK", "non_incr_reason": "op:PIVOT"}
SELECT * FROM (PIVOT STOCK ON S_W_ID IN (1, 2, 3) USING SUM(S_QUANTITY) GROUP BY S_I_ID) p;
