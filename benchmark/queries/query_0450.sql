-- {"operators": "AGGREGATE,FILTER,LIMIT,HAVING", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE"}
SELECT W_ID, SUM(W_TAX) FROM WAREHOUSE WHERE W_TAX > 100 GROUP BY W_ID HAVING SUM(W_TAX) > 10 LIMIT 10;
