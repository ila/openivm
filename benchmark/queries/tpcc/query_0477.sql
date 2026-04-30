-- {"operators": "AGGREGATE,FILTER,LIMIT,HAVING", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE", "ivm_type": "RECOMPUTE"}
SELECT W_ID, SUM(W_TAX) FROM WAREHOUSE WHERE W_TAX < 1 GROUP BY W_ID HAVING SUM(W_TAX) > 1 LIMIT 1;
