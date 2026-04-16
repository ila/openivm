-- {"operators": "AGGREGATE,FILTER,LIMIT,HAVING", "complexity": "medium", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE", "non_incr_reason": "op:LIMIT"}
SELECT W_ID, AVG(W_TAX) FROM WAREHOUSE WHERE W_TAX < 1 GROUP BY W_ID HAVING AVG(W_TAX) > 1 LIMIT 1;
