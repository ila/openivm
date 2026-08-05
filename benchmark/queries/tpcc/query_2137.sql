-- {"operators": "AGGREGATE,HAVING", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "HISTORY", "openivm_verified": true}
SELECT H_W_ID, H_D_ID, COUNT(*) AS payment_count, SUM(H_AMOUNT) AS payment_total FROM HISTORY GROUP BY H_W_ID, H_D_ID HAVING COUNT(*) > 0;
