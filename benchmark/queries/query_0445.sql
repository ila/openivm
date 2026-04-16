-- {"operators": "AGGREGATE,FILTER,LIMIT,HAVING", "complexity": "medium", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE", "non_incr_reason": "op:LIMIT"}
SELECT W_ID, COUNT(W_TAX) FROM WAREHOUSE WHERE W_TAX > 100 GROUP BY W_ID HAVING COUNT(W_TAX) > 0 LIMIT 5;
