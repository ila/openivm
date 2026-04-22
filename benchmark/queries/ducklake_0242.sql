-- {"operators": "AGGREGATE,WINDOW,CTE,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE", "ducklake": true}
WITH w_stats AS (SELECT W_ID AS w, W_YTD AS ytd FROM dl.WAREHOUSE) SELECT w, ytd, AVG(ytd) OVER () AS overall_avg, ytd - AVG(ytd) OVER () AS dev FROM w_stats;
