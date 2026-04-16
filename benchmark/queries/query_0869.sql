-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE"}
SELECT W_STATE, SUM(W_YTD) AS total FROM WAREHOUSE GROUP BY W_STATE;
