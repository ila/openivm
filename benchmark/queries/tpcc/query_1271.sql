-- {"operators": "AGGREGATE,WINDOW", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "DISTRICT", "openivm_verified": true}
SELECT D_W_ID, D_YTD, AVG(D_YTD) OVER (PARTITION BY D_W_ID) AS part_avg FROM DISTRICT;
