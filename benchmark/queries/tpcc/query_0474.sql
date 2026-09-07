-- {"operators": "AGGREGATE,FILTER,LIMIT,HAVING", "complexity": "medium", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE", "refresh_type": "FULL_REFRESH", "mv_verified": true}
SELECT W_ID, MAX(W_TAX) FROM WAREHOUSE WHERE W_TAX < 0 GROUP BY W_ID HAVING MAX(W_TAX) < 10 LIMIT 10;
