-- {"operators": "AGGREGATE,FILTER,LIMIT,HAVING", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE", "ivm_type": "RECOMPUTE"}
SELECT W_ID, AVG(W_TAX) FROM WAREHOUSE WHERE W_TAX > 100 GROUP BY W_ID HAVING AVG(W_TAX) < 10 LIMIT 10;
