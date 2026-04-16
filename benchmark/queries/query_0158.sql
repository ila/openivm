-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE"}
SELECT W_ID, W_YTD, COUNT(*) FROM WAREHOUSE GROUP BY W_ID, W_YTD;
