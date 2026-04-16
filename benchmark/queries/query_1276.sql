-- {"operators": "ORDER,WINDOW", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE", "openivm_verified": true}
SELECT W_STATE, W_YTD, RANK() OVER (PARTITION BY W_STATE ORDER BY W_YTD) AS rnk FROM WAREHOUSE;
