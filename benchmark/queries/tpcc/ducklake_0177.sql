-- {"operators": "AGGREGATE,FILTER,WINDOW", "complexity": "medium", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "HISTORY", "ducklake": true}
SELECT H_W_ID, H_D_ID, H_C_ID, H_AMOUNT, SUM(H_AMOUNT) OVER (PARTITION BY H_W_ID) AS w_total, H_AMOUNT / NULLIF(SUM(H_AMOUNT) OVER (PARTITION BY H_W_ID), 0) AS share FROM dl.HISTORY WHERE H_AMOUNT > 0;
