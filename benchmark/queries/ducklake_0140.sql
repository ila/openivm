-- {"operators": "CROSS_JOIN,FILTER,CTE,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,ITEM", "ducklake": true}
WITH totals AS (SELECT W_ID, W_YTD FROM dl.WAREHOUSE) SELECT t.W_ID, i.I_ID, (t.W_YTD * i.I_PRICE) AS product FROM totals t CROSS JOIN dl.ITEM i WHERE i.I_ID < 5;
