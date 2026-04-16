-- {"operators": "AGGREGATE,WINDOW", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE", "openivm_verified": true}
SELECT W_STATE, W_YTD, W_YTD - AVG(W_YTD) OVER () AS diff_from_avg FROM WAREHOUSE;
