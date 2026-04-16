-- {"operators": "AGGREGATE,ORDER,WINDOW", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "STOCK", "openivm_verified": true}
SELECT S_W_ID, COUNT(*) AS items, PERCENT_RANK() OVER (ORDER BY COUNT(*)) AS pr FROM STOCK GROUP BY S_W_ID;
