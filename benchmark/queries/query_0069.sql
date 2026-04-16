-- {"operators": "AGGREGATE,FILTER", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE"}
SELECT COUNT(*) FROM WAREHOUSE WHERE W_TAX > 0.04 AND W_YTD > 1000;
