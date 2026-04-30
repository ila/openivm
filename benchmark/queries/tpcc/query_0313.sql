-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE"}
SELECT W_ID, COUNT(*) as cnt, SUM(W_YTD) as sum_ytd, AVG(W_TAX) as avg_tax, MIN(W_YTD) as min_ytd, MAX(W_TAX) as max_tax FROM WAREHOUSE GROUP BY W_ID;
