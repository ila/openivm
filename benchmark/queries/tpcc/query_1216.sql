-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": true, "tables": "HISTORY"}
SELECT H_W_ID, SUM(CASE WHEN H_AMOUNT > 100 THEN H_AMOUNT ELSE 0 END) AS big_payments, COUNT(CASE WHEN H_AMOUNT > 100 THEN 1 END) AS big_cnt FROM HISTORY GROUP BY H_W_ID;
