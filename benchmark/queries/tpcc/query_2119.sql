-- {"operators": "SAMPLE,UNNEST,TABLE_FUNCTION", "complexity": "high", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "STOCK", "non_incr_reason": "op:SAMPLE"}
SELECT s.S_W_ID, u.threshold FROM (SELECT * FROM STOCK USING SAMPLE reservoir(15 ROWS) REPEATABLE (19)) s CROSS JOIN UNNEST([10, 50, 90]) AS u(threshold) WHERE s.S_QUANTITY >= u.threshold;
