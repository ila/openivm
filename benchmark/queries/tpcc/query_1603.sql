-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE"}
SELECT W_ID, GREATEST(W_YTD, 0) AS non_neg_ytd, LEAST(W_YTD, 1000000) AS capped_ytd FROM WAREHOUSE;
