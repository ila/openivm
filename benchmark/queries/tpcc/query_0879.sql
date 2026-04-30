-- {"operators": "AGGREGATE,FILTER", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE"}
SELECT W_STATE, COUNT(*) AS cnt, SUM(W_YTD) AS total FROM WAREHOUSE WHERE W_YTD > 0 GROUP BY W_STATE;
