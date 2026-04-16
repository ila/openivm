-- {"operators": "INNER_JOIN,AGGREGATE,TABLE_FUNCTION,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "STOCK"}
SELECT threshold, COUNT(*) AS above_cnt FROM (SELECT unnest([10, 50, 100, 500, 1000]) AS threshold) t JOIN STOCK s ON s.S_QUANTITY >= t.threshold GROUP BY threshold;
