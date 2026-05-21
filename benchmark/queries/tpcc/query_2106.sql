-- {"operators": "PIVOT,AGGREGATE,CTE", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": true, "tables": "STOCK"}
WITH stock_buckets AS (SELECT S_W_ID, CASE WHEN S_QUANTITY < 80 THEN 'low' ELSE 'high' END AS bucket, S_QUANTITY FROM STOCK) SELECT * FROM (PIVOT stock_buckets ON bucket IN ('low', 'high') USING COUNT(*) GROUP BY S_W_ID) p;
