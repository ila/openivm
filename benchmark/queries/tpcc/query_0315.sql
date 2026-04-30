-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "STOCK"}
SELECT S_W_ID, COUNT(*) as item_cnt, SUM(S_QUANTITY) as qty, AVG(S_QUANTITY) as avg_qty, MIN(S_QUANTITY) as min_qty, MAX(S_QUANTITY) as max_qty, SUM(S_YTD) as ytd FROM STOCK GROUP BY S_W_ID;
