-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE"}
SELECT W_STATE, MIN(W_YTD) AS min_val FROM WAREHOUSE GROUP BY W_STATE;
